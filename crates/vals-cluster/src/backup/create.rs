use std::path::Path;
use std::time::Duration;

use crate::partition::RangeMap;
use crate::pd::{placement_equivalent, AddressedMembers, PdInspection, PdStateView};
use crate::zone::{ZoneConfig, ZoneId};

use super::export::{covered, merged_range_tombstones};
use super::*;

pub trait BackupCluster {
    fn capture(&mut self) -> Result<PdStateCapture, String>;
    fn ring(&mut self, members: &AddressedMembers) -> Result<(u32, usize), String>;
    fn sources(
        &mut self,
        members: &AddressedMembers,
        zone_id: ZoneId,
        cfg: &ZoneConfig,
    ) -> Result<Vec<Box<dyn ReplicaSource>>, String>;
}

pub struct NetCluster<F> {
    capture: F,
}

impl<F: FnMut() -> Result<PdStateCapture, String>> NetCluster<F> {
    pub fn new(capture: F) -> Self {
        NetCluster { capture }
    }
}

impl<F: FnMut() -> Result<PdStateCapture, String>> BackupCluster for NetCluster<F> {
    fn capture(&mut self) -> Result<PdStateCapture, String> {
        (self.capture)()
    }

    fn ring(&mut self, members: &AddressedMembers) -> Result<(u32, usize), String> {
        let mut last = "no member has a data address".to_string();
        for (_, _, addrs) in members {
            for addr in addrs {
                match export::ring_params(*addr, Duration::from_secs(10)) {
                    Ok(ring) => return Ok(ring),
                    Err(e) => last = e,
                }
            }
        }
        Err(format!("cannot read the ring parameters from any member: {last}"))
    }

    fn sources(
        &mut self,
        members: &AddressedMembers,
        zone_id: ZoneId,
        cfg: &ZoneConfig,
    ) -> Result<Vec<Box<dyn ReplicaSource>>, String> {
        zone_sources(members, zone_id, cfg)
    }
}

pub fn capture_from_inspection(view: PdInspection) -> Result<PdStateCapture, String> {
    if view.authoritative.is_none() {
        let s = &view.status;
        return Err(format!(
            "pd {} could not confirm itself as the current leader (role {}, leader {}); \
             export PD state from the current leader's admin socket",
            s.id,
            s.role,
            s.leader.map_or_else(|| "unknown".to_string(), |id| id.to_string()),
        ));
    }
    let entries = view.state.ok_or("authoritative inspection carried no PD state")?;
    Ok(PdStateCapture {
        applied: view.status.applied,
        term: view.status.term,
        captured_unix_ms: now_unix_ms(),
        entries,
    })
}

fn range_map_or_single(view: &PdStateView, zone_id: ZoneId) -> RangeMap {
    view.range_maps.get(&zone_id).cloned().unwrap_or_else(RangeMap::single)
}

pub fn placement_change(before: &PdStateView, after: &PdStateView) -> Option<String> {
    if before.incarnation != after.incarnation {
        return Some("the cluster incarnation changed".into());
    }
    if before.members != after.members {
        return Some("data-node membership changed".into());
    }
    if before.zones != after.zones {
        return Some("the zone catalog changed".into());
    }
    let mut ids: Vec<ZoneId> = before.range_maps.keys().chain(after.range_maps.keys()).copied().collect();
    ids.sort_unstable();
    ids.dedup();
    for id in ids {
        if !placement_equivalent(&range_map_or_single(before, id), &range_map_or_single(after, id)) {
            return Some(format!("range placement of zone {id} changed (re-home, balance or token change)"));
        }
    }
    None
}

pub fn create_backup(
    out: &Path,
    cluster: &mut dyn BackupCluster,
    tool: &str,
    progress: &mut dyn FnMut(&str),
) -> Result<Manifest, String> {
    std::fs::create_dir(out).map_err(|e| format!("create {}: {e}", out.display()))?;
    let result = create_into(out, cluster, tool, progress);
    if result.is_err() {
        let _ = std::fs::remove_dir_all(out);
    }
    result
}

fn create_into(
    out: &Path,
    cluster: &mut dyn BackupCluster,
    tool: &str,
    progress: &mut dyn FnMut(&str),
) -> Result<Manifest, String> {
    let first = cluster.capture()?;
    let view = first.view();
    let source_incarnation = view.incarnation.ok_or(
        "the PD has not committed a cluster incarnation yet; wait for its leader to mint one",
    )?;
    if view.members.is_empty() {
        return Err("the PD state has no data-node members".into());
    }
    let (ring_vnodes, ring_rf) = cluster.ring(&view.members)?;
    let pd_bytes = first.encode();
    write_new_file_synced(&out.join(PD_STATE_FILE), &pd_bytes)
        .map_err(|e| format!("write {}: {e}", out.join(PD_STATE_FILE).display()))?;
    progress(&format!(
        "pd state: index {} term {}, {} entries, incarnation {source_incarnation:032x}",
        first.applied, first.term, first.entries.len()
    ));
    let mut zones = Vec::new();
    for (zone_id, cfg) in expected_zones(&view, ring_rf) {
        let mut sources = cluster.sources(&view.members, zone_id, &cfg)?;
        let replicas = sources.len();
        let rts = match cfg.ordering {
            Ordering::Sorted => merged_range_tombstones(&mut sources)
                .map_err(|e| format!("zone {zone_id} ({}): {e}", cfg.name))?,
            Ordering::Unsorted => Default::default(),
        };
        let name = zone_file_name(zone_id);
        let path = out.join(&name);
        let mut writer = create_records(&path, zone_id).map_err(|e| format!("create {}: {e}", path.display()))?;
        let mut omitted = 0u64;
        let stats = merge_replicas_into(sources, &mut |record| {
            if covered(&rts, &record) {
                omitted += 1;
                return Ok(());
            }
            writer.push(&record).map_err(|e| format!("write backup record: {e}"))
        })
        .map_err(|e| format!("zone {zone_id} ({}): {e}", cfg.name))?;
        for fragment in rts.fragments() {
            writer
                .push_range_tombstone(&crate::rt_sync::to_wire(fragment))
                .map_err(|e| format!("write {}: {e}", path.display()))?;
        }
        let (file, summary) = writer.finish().map_err(|e| format!("write {}: {e}", path.display()))?;
        file.into_inner()
            .map_err(|e| format!("write {}: {e}", path.display()))?
            .sync_all()
            .map_err(|e| format!("sync {}: {e}", path.display()))?;
        progress(&format!(
            "zone {zone_id} ({}): {} records ({} put, {} tombstone), {} range tombstones ({omitted} covered records \
             omitted) from {} replica entries on {replicas} replica shards",
            cfg.name,
            summary.counts.records,
            summary.counts.puts,
            summary.counts.tombstones,
            summary.range_tombstones,
            stats.replica_entries
        ));
        zones.push(ZoneEntry {
            id: zone_id,
            name: cfg.name.clone(),
            ordering: cfg.ordering,
            rf: cfg.rf,
            file: FileEntry { name, bytes: summary.bytes, crc32c: summary.crc32c },
            counts: summary.counts,
        });
    }
    let last = cluster.capture()?;
    if let Some(change) = placement_change(&view, &last.view()) {
        return Err(format!(
            "{change} between PD index {} and {} while the backup ran; the copy may be incomplete \
             and was discarded; retry when topology is quiet",
            first.applied, last.applied
        ));
    }
    let manifest = Manifest {
        backup_id: random_id().map_err(|e| format!("draw a backup id: {e}"))?,
        created_unix_ms: now_unix_ms(),
        tool: tool.to_string(),
        source_incarnation,
        pd_applied: first.applied,
        pd_applied_end: last.applied,
        ring_vnodes,
        ring_rf,
        pd_state: FileEntry {
            name: PD_STATE_FILE.to_string(),
            bytes: pd_bytes.len() as u64,
            crc32c: vals_store::crc::crc32(&pd_bytes),
        },
        zones,
    };
    write_new_file_synced(&out.join(MANIFEST_FILE), manifest.render().as_bytes())
        .map_err(|e| format!("write {}: {e}", out.join(MANIFEST_FILE).display()))?;
    Ok(manifest)
}
