use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::partition::RangeMap;
use crate::pd::{
    placement_equivalent, restore_seed_of, restored_state, seed_voters_entry, without_pending, AddressedMembers,
    Lineage, PdStateView,
};
use vals_store::range_tombstone::FragmentSet;

use crate::replica::pack_version;
use crate::ring::{NodeId, Ring};
use crate::wire::{RtFragment, WireSlice};
use crate::zone::{ZoneConfig, ZoneId, DEFAULT_ZONE};
use crate::{Applied, BatchAck, BatchEntry, Coordinator, NetTransport, Transport};

use super::create::{placement_change, BackupCluster};
use super::export::{covered, key_text, merged_range_tombstones};
use super::*;

const LOAD_BATCH: usize = 256;
const LOAD_RANGE_CONCURRENCY: usize = 32;

type OwnedSlice = (Option<Vec<u8>>, Option<Vec<u8>>);
const LOAD_BATCH_BYTES: usize = 8 << 20;
const RECORD_WIRE_OVERHEAD: usize = 32;
const LOAD_RETRY_FOR: Duration = Duration::from_secs(120);
const ADOPT_WAIT: Duration = Duration::from_secs(120);
const MAX_SAMPLES: usize = 20;

fn run<F: std::future::Future>(f: F) -> F::Output {
    vals_rt::block_on(f, crate::reactor::poll_io)
}

fn member_weights(members: &AddressedMembers) -> Vec<(NodeId, u32)> {
    let mut weights: Vec<(NodeId, u32)> = members.iter().map(|(node, weight, _)| (*node, *weight)).collect();
    weights.sort_unstable();
    weights
}

fn map_or_single(view: &PdStateView, zone_id: ZoneId) -> RangeMap {
    view.range_maps.get(&zone_id).cloned().unwrap_or_else(RangeMap::single)
}

pub fn lineage_of(manifest: &Manifest) -> Lineage {
    Lineage {
        predecessor: manifest.source_incarnation,
        backup_id: manifest.backup_id,
        source_applied: manifest.pd_applied,
    }
}

pub struct RestoreTarget {
    pub raft_dir: PathBuf,
    pub voters: Vec<u64>,
    pub members: AddressedMembers,
    pub ring: (u32, usize),
    pub boot_zones: Vec<ZoneConfig>,
    pub opaque_range_maps: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestoredPd {
    pub backup_id: u128,
    pub incarnation: u128,
    pub lineage: Lineage,
    pub index: u64,
    pub term: u64,
    pub entries: usize,
    pub digest: u32,
}

pub fn restore_pd(dir: &Path, target: &RestoreTarget, incarnation: u128) -> Result<RestoredPd, String> {
    let backup = open_backup(dir)?;
    let manifest = &backup.manifest;
    if target.ring != (manifest.ring_vnodes, manifest.ring_rf) {
        return Err(format!(
            "the target PD config has vnodes {} rf {}, the backup's ring is vnodes {} rf {}; \
             placement would differ",
            target.ring.0, target.ring.1, manifest.ring_vnodes, manifest.ring_rf
        ));
    }
    let view = backup.pd_state.view();
    for zone in &target.boot_zones {
        let Some(cfg) = view.zones.by_name(&zone.name).and_then(|id| view.zones.get(id)) else {
            return Err(format!(
                "the target PD config declares boot zone {:?}, which the backup does not have; \
                 the PD would create it on start",
                zone.name
            ));
        };
        if cfg.rf != zone.rf || cfg.ordering != zone.ordering {
            return Err(format!(
                "the target PD config declares boot zone {:?} with rf {} {:?}, the backup has rf {} {:?}",
                zone.name, zone.rf, zone.ordering, cfg.rf, cfg.ordering
            ));
        }
    }
    if !target.opaque_range_maps && view.range_maps.values().any(RangeMap::supports_opaque_tokens) {
        return Err(
            "the backup holds opaque-token range maps; the target PD must run with opaque_range_maps = true".into(),
        );
    }
    let lineage = lineage_of(manifest);
    let mut entries = restored_state(&backup.pd_state.entries, &target.members, incarnation, lineage)?;
    let mut voters = target.voters.clone();
    voters.sort_unstable();
    voters.dedup();
    let seed = seed_voters_entry(&voters);
    let at = entries.partition_point(|(key, _)| *key < seed.0);
    entries.insert(at, seed);
    let index = backup.pd_state.applied.max(1);
    let term = backup.pd_state.term.max(1);
    let encoded = PdStateCapture { applied: index, term, captured_unix_ms: 0, entries: entries.clone() }.encode();
    let digest = vals_store::crc::crc32(&encoded[..encoded.len() - 4]);
    vals_raft::seed_snapshot(&target.raft_dir, target.voters.clone(), index, term, &entries)
        .map_err(|e| format!("seed {}: {e}", target.raft_dir.display()))?;
    Ok(RestoredPd {
        backup_id: manifest.backup_id,
        incarnation,
        lineage,
        index,
        term,
        entries: entries.len(),
        digest,
    })
}

pub fn check_restored(backup: &Backup, target: &PdStateView) -> Result<(), String> {
    let manifest = &backup.manifest;
    match target.lineage {
        Some(lineage) if lineage == lineage_of(manifest) => {}
        Some(lineage) => {
            return Err(format!(
                "the target PD was restored from backup {:032x} (source incarnation {:032x}), \
                 not from this backup {:032x}",
                lineage.backup_id, lineage.predecessor, manifest.backup_id
            ))
        }
        None => {
            return Err(
                "the target PD state records no restore lineage; load and compare only run against a PD \
                 seeded from this backup by `valsd backup restore-pd`"
                    .into(),
            )
        }
    }
    if target.incarnation.is_none_or(|id| id == manifest.source_incarnation) {
        return Err("the target PD does not hold a cluster incarnation of its own".into());
    }
    let source = backup.pd_state.view();
    if member_weights(&source.members) != member_weights(&target.members) {
        return Err(format!(
            "the target membership (id, weight) {:?} differs from the backup's {:?}",
            member_weights(&target.members),
            member_weights(&source.members)
        ));
    }
    if source.zones != target.zones {
        return Err("the target zone catalog differs from the backup's".into());
    }
    let ids: BTreeSet<ZoneId> = source.range_maps.keys().chain(target.range_maps.keys()).copied().collect();
    for id in ids {
        if !placement_equivalent(&without_pending(&map_or_single(&source, id)), &map_or_single(target, id)) {
            return Err(format!("the target range placement of zone {id} differs from the backup's"));
        }
    }
    Ok(())
}

#[derive(Debug)]
pub enum SeedStatus {
    Confirmed,
    Waiting(String),
}

pub fn check_seed(entries: &[(Vec<u8>, Vec<u8>)]) -> Result<SeedStatus, String> {
    let Some(seed) = restore_seed_of(entries) else {
        return Err("the target PD state holds no restore seed record (`W`); it was seeded by a build that \
                    predates seed confirmation: stop every PD voter, empty each PD data_dir and rerun restore-pd \
                    with this build"
            .into());
    };
    if seed.voters.len() <= 1 {
        return Ok(SeedStatus::Confirmed);
    }
    let digests: BTreeSet<Option<u32>> = seed.acks.values().copied().collect();
    if digests.len() > 1 || digests.contains(&None) {
        let acks: Vec<String> = seed
            .acks
            .iter()
            .map(|(voter, digest)| match digest {
                Some(digest) => format!("voter {voter} {digest:08x}"),
                None => format!("voter {voter} undecodable"),
            })
            .collect();
        return Err(format!(
            "the target PD voters were seeded differently by restore-pd (state digests: {}), so they hold \
             divergent PD state: stop every voter, empty each PD data_dir and rerun restore-pd on every voter \
             with the same backup, --incarnation and data_node lines",
            acks.join(", ")
        ));
    }
    let missing: Vec<u64> = seed.voters.iter().copied().filter(|voter| !seed.acks.contains_key(voter)).collect();
    if missing.is_empty() {
        return Ok(SeedStatus::Confirmed);
    }
    Ok(SeedStatus::Waiting(format!(
        "PD voter(s) {missing:?} of {:?} have not confirmed their restore seed (each voter confirms once it \
         runs with the `restore-seed` file restore-pd wrote into its data_dir and the group has a leader)",
        seed.voters
    )))
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LoadReport {
    pub zones: usize,
    pub counts: RecordCounts,
    pub range_tombstones: u64,
}

fn node_mismatch(
    coord: &Coordinator<NetTransport>,
    node: NodeId,
    view: &PdStateView,
    ring: (u32, usize),
) -> Option<String> {
    let transport = coord.transport();
    let served = match run(transport.members(node)) {
        Ok(served) => served,
        Err(e) => return Some(format!("node {node} membership: {e:?}")),
    };
    let mut members = served.members.clone();
    members.sort_unstable();
    if members != member_weights(&view.members) || (served.vnodes, served.rf as usize) != ring {
        return Some(format!(
            "node {node} serves membership {members:?} with vnodes {} rf {}",
            served.vnodes, served.rf
        ));
    }
    let zones = match run(transport.zones(node)) {
        Ok(zones) => zones,
        Err(e) => return Some(format!("node {node} zones: {e:?}")),
    };
    for (id, cfg) in view.zones.iter() {
        if id != DEFAULT_ZONE && zones.get(id) != Some(cfg) {
            return Some(format!("node {node} has not adopted zone {id} ({})", cfg.name));
        }
    }
    let maps: HashMap<ZoneId, RangeMap> = match run(transport.range_maps(node)) {
        Ok(maps) => maps.into_iter().collect(),
        Err(e) => return Some(format!("node {node} range maps: {e:?}")),
    };
    for (id, map) in &view.range_maps {
        if !maps.get(id).is_some_and(|served| placement_equivalent(map, served)) {
            return Some(format!("node {node} has not adopted the range map of zone {id}"));
        }
    }
    None
}

fn load_batch(coord: &Coordinator<NetTransport>, zone_id: ZoneId, records: &[Record]) -> Result<(), String> {
    let deadline = Instant::now() + LOAD_RETRY_FOR;
    let mut backoff = Duration::from_millis(20);
    let mut pending: Vec<usize> = (0..records.len()).collect();
    loop {
        let mut by_node: BTreeMap<NodeId, Vec<usize>> = BTreeMap::new();
        for &i in &pending {
            let replicas = coord
                .replicas_for(zone_id, &records[i].key)
                .map_err(|e| format!("zone {zone_id} key {}: {e}", key_text(&records[i].key)))?;
            for node in replicas {
                by_node.entry(node).or_default().push(i);
            }
        }
        let sends = by_node
            .iter()
            .map(|(&node, indices)| {
                let entries: Vec<BatchEntry<'_>> = indices
                    .iter()
                    .map(|&i| BatchEntry {
                        key: &records[i].key,
                        value: records[i].value.as_deref(),
                        version: records[i].version,
                    })
                    .collect();
                async move { coord.transport().apply_batch(node, zone_id, true, &entries).await }
            })
            .collect();
        let replies = run(vals_rt::join_all(sends));
        let mut failed = BTreeSet::new();
        let mut stale = false;
        let mut last = String::new();
        for ((node, indices), acks) in by_node.iter().zip(replies) {
            for (j, &i) in indices.iter().enumerate() {
                let record = &records[i];
                match acks.get(j) {
                    Some(BatchAck::Applied(Applied::Won)) => {}
                    Some(BatchAck::Applied(Applied::Lost(held))) if *held == record.version => {}
                    Some(BatchAck::Applied(Applied::Lost(held))) => {
                        return Err(format!(
                            "node {node} already holds version {held:?} of zone {zone_id} key {}, newer than \
                             the backup's {:?}; restore into empty, freshly provisioned nodes",
                            key_text(&record.key),
                            record.version
                        ))
                    }
                    Some(BatchAck::Applied(Applied::Rejected)) => {
                        return Err(format!(
                            "node {node} refused zone {zone_id} key {} at version {:?} on clock skew",
                            key_text(&record.key),
                            record.version
                        ))
                    }
                    Some(BatchAck::Stale(epoch)) => {
                        stale = true;
                        failed.insert(i);
                        last = format!("node {node}: stale placement (node epoch {epoch})");
                    }
                    other => {
                        failed.insert(i);
                        last = format!("node {node}: {other:?}");
                    }
                }
            }
        }
        if failed.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "zone {zone_id}: {} records not acknowledged by every replica within {LOAD_RETRY_FOR:?} \
                 (last: {last})",
                failed.len()
            ));
        }
        log::debug!("backup load: zone {zone_id}: {} records to retry ({last})", failed.len());
        if stale {
            run(coord.adopt_from_any_member());
        }
        std::thread::sleep(backoff);
        backoff = (backoff * 2).min(Duration::from_secs(2));
        pending = failed.into_iter().collect();
    }
}

fn span_text(rt: &RtFragment) -> String {
    format!(
        "[{}, {})",
        rt.lo.as_deref().map_or_else(|| "-inf".to_string(), key_text),
        rt.hi.as_deref().map_or_else(|| "+inf".to_string(), key_text)
    )
}

fn load_range_tombstones(
    coord: &Coordinator<NetTransport>,
    zone_id: ZoneId,
    rts: &[RtFragment],
) -> Result<(), String> {
    let deadline = Instant::now() + LOAD_RETRY_FOR;
    let mut backoff = Duration::from_millis(20);
    let mut pending: Vec<usize> = (0..rts.len()).collect();
    loop {
        let map = coord.range_map(zone_id);
        let mut sends: Vec<(usize, NodeId, Vec<OwnedSlice>)> = Vec::new();
        for &i in &pending {
            let mut by_node: BTreeMap<NodeId, Vec<OwnedSlice>> = BTreeMap::new();
            for slice in map.ranges_overlapping(rts[i].lo.as_deref(), rts[i].hi.as_deref()) {
                let replicas = coord
                    .replicas_for(zone_id, slice.lo.as_deref().unwrap_or_default())
                    .map_err(|e| format!("zone {zone_id} range tombstone {}: {e}", span_text(&rts[i])))?;
                for node in replicas {
                    by_node.entry(node).or_default().push((slice.lo.clone(), slice.hi.clone()));
                }
            }
            sends.extend(by_node.into_iter().map(|(node, slices)| (i, node, slices)));
        }
        let mut failed = BTreeSet::new();
        let mut stale = false;
        let mut last = String::new();
        for group in sends.chunks(LOAD_RANGE_CONCURRENCY) {
            let futs = group
                .iter()
                .map(|(i, node, slices)| async move {
                    let wire: Vec<WireSlice<'_>> = slices.iter().map(|(lo, hi)| (lo.as_deref(), hi.as_deref())).collect();
                    coord.transport().range_delete(*node, zone_id, true, rts[*i].version, &wire).await
                })
                .collect();
            let replies = run(vals_rt::join_all(futs));
            for ((i, node, slices), acks) in group.iter().zip(replies) {
                let rt = &rts[*i];
                for j in 0..slices.len() {
                    match acks.get(j) {
                        Some(BatchAck::Applied(Applied::Won | Applied::Lost(_))) => {}
                        Some(BatchAck::Applied(Applied::Rejected)) => {
                            return Err(format!(
                                "node {node} refused zone {zone_id} range tombstone {} at version {:?} on clock skew",
                                span_text(rt),
                                rt.version
                            ))
                        }
                        Some(BatchAck::Stale(epoch)) => {
                            stale = true;
                            failed.insert(*i);
                            last = format!("node {node}: stale placement (node epoch {epoch})");
                        }
                        other => {
                            failed.insert(*i);
                            last = format!("node {node}: {other:?}");
                        }
                    }
                }
            }
        }
        if failed.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "zone {zone_id}: {} range tombstones not acknowledged by every replica within {LOAD_RETRY_FOR:?} \
                 (last: {last})",
                failed.len()
            ));
        }
        log::debug!("backup load: zone {zone_id}: {} range tombstones to retry ({last})", failed.len());
        if stale {
            run(coord.adopt_from_any_member());
        }
        std::thread::sleep(backoff);
        backoff = (backoff * 2).min(Duration::from_secs(2));
        pending = failed.into_iter().collect();
    }
}

fn record_wire_bytes(record: &Record) -> usize {
    record.key.len() + record.value.as_ref().map_or(0, Vec::len) + RECORD_WIRE_OVERHEAD
}

pub(super) fn next_batch<R: std::io::Read>(
    reader: &mut RecordReader<R>,
    carry: &mut Option<Record>,
    max_records: usize,
    max_bytes: usize,
) -> Result<Vec<Record>, String> {
    let mut batch = Vec::new();
    let mut bytes = 0usize;
    loop {
        let record = match carry.take() {
            Some(record) => record,
            None => match reader.next_record()? {
                Some(record) => record,
                None => return Ok(batch),
            },
        };
        let size = record_wire_bytes(&record);
        if !batch.is_empty() && (batch.len() >= max_records || bytes + size > max_bytes) {
            *carry = Some(record);
            return Ok(batch);
        }
        bytes += size;
        batch.push(record);
    }
}

pub fn load_backup(
    dir: &Path,
    capture: &mut dyn FnMut() -> Result<PdStateCapture, String>,
    progress: &mut dyn FnMut(&str),
) -> Result<LoadReport, String> {
    let backup = open_backup(dir)?;
    let manifest = &backup.manifest;
    let mut first = capture()?;
    check_restored(&backup, &first.view())?;
    let deadline = Instant::now() + ADOPT_WAIT;
    while let SeedStatus::Waiting(waiting) = check_seed(&first.entries)? {
        if Instant::now() >= deadline {
            return Err(format!("{waiting}; waited {ADOPT_WAIT:?}"));
        }
        log::debug!("backup load: {waiting}");
        std::thread::sleep(Duration::from_millis(500));
        first = capture()?;
        check_restored(&backup, &first.view())?;
    }
    let view = first.view();
    let ring = (manifest.ring_vnodes, manifest.ring_rf);
    let transport =
        NetTransport::connect_sharded(view.members.iter().map(|(node, _, addrs)| (*node, addrs.clone())));
    let coord = Coordinator::new(0, Ring::new(manifest.ring_rf), 1, transport);
    let deadline = Instant::now() + ADOPT_WAIT;
    loop {
        let waiting = view.members.iter().find_map(|(node, _, _)| node_mismatch(&coord, *node, &view, ring));
        let Some(waiting) = waiting else { break };
        if Instant::now() >= deadline {
            return Err(format!(
                "the target data nodes did not adopt the restored PD's configuration within {ADOPT_WAIT:?}: \
                 {waiting}"
            ));
        }
        log::debug!("backup load: waiting for the target nodes: {waiting}");
        std::thread::sleep(Duration::from_millis(200));
    }
    let first_node = view.members[0].0;
    run(coord.refresh_ring(first_node)).map_err(|e| format!("node {first_node}: {e}"))?;
    run(coord.refresh_catalog(first_node)).map_err(|e| format!("node {first_node}: {e}"))?;
    progress(&format!(
        "target PD index {} incarnation {:032x}: every data node serves the restored configuration",
        first.applied,
        view.incarnation.unwrap_or_default()
    ));
    let mut report = LoadReport::default();
    for zone in &manifest.zones {
        let path = dir.join(&zone.file.name);
        let mut reader = open_records(&path)?;
        if reader.zone_id() != zone.id {
            return Err(format!("{}: holds zone {}, MANIFEST expects {}", path.display(), reader.zone_id(), zone.id));
        }
        let mut carry = None;
        loop {
            let batch = next_batch(&mut reader, &mut carry, LOAD_BATCH, LOAD_BATCH_BYTES)
                .map_err(|e| format!("{}: {e}", path.display()))?;
            if batch.is_empty() {
                break;
            }
            load_batch(&coord, zone.id, &batch)?;
        }
        check_file(dir, &zone.file, reader.file_summary().expect("finished reader"))?;
        if reader.counts() != zone.counts {
            return Err(format!("{}: holds {:?}, MANIFEST records {:?}", path.display(), reader.counts(), zone.counts));
        }
        let rts = reader.range_tombstones();
        if !rts.is_empty() {
            if zone.ordering != Ordering::Sorted {
                return Err(format!("{}: holds range tombstones for unsorted zone {}", path.display(), zone.id));
            }
            load_range_tombstones(&coord, zone.id, rts)?;
        }
        progress(&format!(
            "zone {} ({}): loaded {} records ({} put, {} tombstone) and {} range tombstones",
            zone.id,
            zone.name,
            zone.counts.records,
            zone.counts.puts,
            zone.counts.tombstones,
            rts.len()
        ));
        report.zones += 1;
        report.range_tombstones += rts.len() as u64;
        report.counts.records += zone.counts.records;
        report.counts.puts += zone.counts.puts;
        report.counts.tombstones += zone.counts.tombstones;
    }
    let last = capture()?;
    if let Some(change) = placement_change(&view, &last.view()) {
        return Err(format!(
            "{change} between target PD index {} and {} while loading; rerun load (it is idempotent) \
             when topology is quiet",
            first.applied, last.applied
        ));
    }
    Ok(report)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ZoneComparison {
    pub id: ZoneId,
    pub matched: u64,
    pub matched_range_tombstones: u64,
    pub collected_tombstones: u64,
    pub differences: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompareReport {
    pub zones: Vec<ZoneComparison>,
    pub samples: Vec<String>,
}

impl CompareReport {
    pub fn differences(&self) -> u64 {
        self.zones.iter().map(|zone| zone.differences).sum()
    }
}

fn describe(record: &Record) -> String {
    match &record.value {
        Some(value) => format!("a {}-byte put at {:?}", value.len(), record.version),
        None => format!("a tombstone at {:?}", record.version),
    }
}

struct ZoneComparer<'a, R: std::io::Read> {
    reader: RecordReader<R>,
    path: PathBuf,
    next: Option<Record>,
    cmp: ZoneComparison,
    samples: &'a mut Vec<String>,
}

impl<R: std::io::Read> ZoneComparer<'_, R> {
    fn pull(&mut self) -> Result<(), String> {
        self.next = self.reader.next_record().map_err(|e| format!("{}: {e}", self.path.display()))?;
        Ok(())
    }

    fn differ(&mut self, what: String) {
        self.cmp.differences += 1;
        if self.samples.len() < MAX_SAMPLES {
            self.samples.push(format!("zone {}: {what}", self.cmp.id));
        }
    }

    fn backup_only(&mut self, record: Record) {
        if record.value.is_some() {
            self.differ(format!("key {}: the backup holds {}, the target has no such key", key_text(&record.key), describe(&record)));
        } else {
            self.cmp.collected_tombstones += 1;
        }
    }

    fn target(&mut self, record: Record) -> Result<(), String> {
        while self.next.as_ref().is_some_and(|held| held.key < record.key) {
            let held = self.next.take().expect("checked above");
            self.backup_only(held);
            self.pull()?;
        }
        match self.next.take() {
            Some(held) if held.key == record.key => {
                if held == record {
                    self.cmp.matched += 1;
                } else {
                    self.differ(format!(
                        "key {}: the backup holds {}, the target holds {}",
                        key_text(&record.key),
                        describe(&held),
                        describe(&record)
                    ));
                }
                self.pull()?;
            }
            other => {
                self.next = other;
                self.differ(format!("key {}: the target holds {}, the backup has no such key", key_text(&record.key), describe(&record)));
            }
        }
        Ok(())
    }

    fn finish(mut self, dir: &Path, zone: &ZoneEntry, target_rts: &FragmentSet) -> Result<ZoneComparison, String> {
        while let Some(held) = self.next.take() {
            self.backup_only(held);
            self.pull()?;
        }
        let mut backup_rts = FragmentSet::with_max_fragments(usize::MAX);
        for rt in self.reader.range_tombstones() {
            backup_rts
                .overlay(rt.lo.as_deref(), rt.hi.as_deref(), pack_version(rt.version))
                .expect("an uncapped overlay never refuses");
        }
        let target: Vec<RtFragment> = target_rts.fragments().map(crate::rt_sync::to_wire).collect();
        let held: Vec<RtFragment> = backup_rts.fragments().map(crate::rt_sync::to_wire).collect();
        for rt in &held {
            if target.contains(rt) {
                self.cmp.matched_range_tombstones += 1;
            } else {
                self.differ(format!(
                    "range tombstone {} at {:?}: the backup holds it, the target does not",
                    span_text(rt),
                    rt.version
                ));
            }
        }
        for rt in target.iter().filter(|rt| !held.contains(rt)) {
            self.differ(format!(
                "range tombstone {} at {:?}: the target holds it, the backup does not",
                span_text(rt),
                rt.version
            ));
        }
        check_file(dir, &zone.file, self.reader.file_summary().expect("finished reader"))?;
        if self.reader.counts() != zone.counts {
            return Err(format!(
                "{}: holds {:?}, MANIFEST records {:?}",
                self.path.display(),
                self.reader.counts(),
                zone.counts
            ));
        }
        Ok(self.cmp)
    }
}

pub fn compare_backup(
    dir: &Path,
    cluster: &mut dyn BackupCluster,
    progress: &mut dyn FnMut(&str),
) -> Result<CompareReport, String> {
    let backup = open_backup(dir)?;
    let manifest = &backup.manifest;
    let first = cluster.capture()?;
    let view = first.view();
    check_restored(&backup, &view)?;
    if let SeedStatus::Waiting(waiting) = check_seed(&first.entries)? {
        return Err(waiting);
    }
    let ring = cluster.ring(&view.members)?;
    if ring != (manifest.ring_vnodes, manifest.ring_rf) {
        return Err(format!(
            "the target ring is vnodes {} rf {}, the backup's is vnodes {} rf {}",
            ring.0, ring.1, manifest.ring_vnodes, manifest.ring_rf
        ));
    }
    let mut report = CompareReport::default();
    let zones = expected_zones(&view, ring.1);
    for (zone, (zone_id, cfg)) in manifest.zones.iter().zip(&zones) {
        let path = dir.join(&zone.file.name);
        let mut comparer = ZoneComparer {
            reader: open_records(&path)?,
            path: path.clone(),
            next: None,
            cmp: ZoneComparison { id: zone.id, ..ZoneComparison::default() },
            samples: &mut report.samples,
        };
        comparer.pull()?;
        let mut sources = cluster.sources(&view.members, *zone_id, cfg)?;
        let target_rts = match cfg.ordering {
            Ordering::Sorted => merged_range_tombstones(&mut sources)
                .map_err(|e| format!("zone {zone_id} ({}): {e}", cfg.name))?,
            Ordering::Unsorted => FragmentSet::default(),
        };
        let unfenced = None;
        merge_replicas_into(sources, unfenced, &mut |record| {
            if covered(&target_rts, &record) {
                return Ok(());
            }
            comparer.target(record)
        })
        .map_err(|e| format!("zone {zone_id} ({}): {e}", cfg.name))?;
        let cmp = comparer.finish(dir, zone, &target_rts)?;
        progress(&format!(
            "zone {} ({}): {} records and {} range tombstones match, {} backup tombstones already collected, \
             {} differences",
            zone.id, zone.name, cmp.matched, cmp.matched_range_tombstones, cmp.collected_tombstones, cmp.differences
        ));
        report.zones.push(cmp);
    }
    let last = cluster.capture()?;
    if let Some(change) = placement_change(&view, &last.view()) {
        return Err(format!(
            "{change} between target PD index {} and {} while comparing; retry when topology is quiet",
            first.applied, last.applied
        ));
    }
    Ok(report)
}
