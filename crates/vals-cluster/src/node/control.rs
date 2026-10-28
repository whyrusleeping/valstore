use super::*;

/// A running data node; see [`Node`].
pub struct NodeHandle {
    pub(super) shards: Vec<ShardHandle>,
    pub(super) maint_join: crate::maint::MaintJoin,
}

pub(super) struct ShardHandle {
    pub(super) stop: Arc<crate::reactor::Notify>,
    pub(super) ctl: mpsc::Sender<NodeCtl>,
    pub(super) ctl_notify: Arc<crate::reactor::Notify>,
    pub(super) join: std::thread::JoinHandle<()>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZoneSegmentStats {
    pub zone_id: ZoneId,
    pub used_segments: u64,
    pub free_segments: u64,
    pub live_keys: u64,
    pub resident_runs: usize,
}

pub enum NodeCtl {
    Dump {
        zone: String,
        out: PathBuf,
        skip_tombstones: bool,
        parallel: usize,
        rate_mib: Option<u64>,
        reply: mpsc::Sender<Result<String, String>>,
    },
    ZoneStats {
        reply: mpsc::Sender<Vec<ZoneSegmentStats>>,
    },
    SetSweepEvery(Duration),
    #[cfg(test)]
    SweepCounts {
        reply: mpsc::Sender<(u64, u64)>,
    },
    SwapStore {
        dev: Box<dyn std::any::Any + Send>,
        geom: crate::drive::PartitionGeom,
        node: NodeId,
        prepared: mpsc::Sender<Result<(), String>>,
        commit: mpsc::Receiver<()>,
        reply: mpsc::Sender<Result<(), String>>,
    },
}

#[derive(Clone)]
pub struct NodeDumper {
    shards: Vec<(mpsc::Sender<NodeCtl>, Arc<crate::reactor::Notify>)>,
}

impl NodeDumper {
    pub fn all_zone_stats(&self) -> Result<Vec<(usize, ZoneSegmentStats)>, String> {
        let mut all = Vec::new();
        for shard in 0..self.shards.len() {
            all.extend(
                self.zone_stats(shard)?
                    .into_iter()
                    .map(|stats| (shard, stats)),
            );
        }
        Ok(all)
    }

    pub fn zone_stats(&self, shard: usize) -> Result<Vec<ZoneSegmentStats>, String> {
        let (tx, notify) = self.shards.get(shard).ok_or_else(|| {
            format!(
                "shard {shard} out of range (node has {})",
                self.shards.len()
            )
        })?;
        let (reply_tx, reply_rx) = mpsc::channel();
        tx.send(NodeCtl::ZoneStats { reply: reply_tx })
            .map_err(|_| "node shard is gone (shutting down?)".to_string())?;
        notify.signal();
        reply_rx
            .recv()
            .map_err(|_| "node shard dropped the zone-stats reply".to_string())
    }

    #[cfg(test)]
    pub(crate) fn sweep_counts(&self, shard: usize) -> (u64, u64) {
        let (tx, notify) = &self.shards[shard];
        let (reply_tx, reply_rx) = mpsc::channel();
        tx.send(NodeCtl::SweepCounts { reply: reply_tx }).unwrap();
        notify.signal();
        reply_rx.recv().unwrap()
    }

    pub fn dump_zone(
        &self,
        shard: usize,
        zone: String,
        out: PathBuf,
        skip_tombstones: bool,
        parallel: usize,
        rate_mib: Option<u64>,
    ) -> Result<String, String> {
        let (tx, notify) = self.shards.get(shard).ok_or_else(|| {
            format!(
                "shard {shard} out of range (node has {})",
                self.shards.len()
            )
        })?;
        let (reply_tx, reply_rx) = mpsc::channel();
        tx.send(NodeCtl::Dump {
            zone,
            out,
            skip_tombstones,
            parallel,
            rate_mib,
            reply: reply_tx,
        })
        .map_err(|_| "node shard is gone (shutting down?)".to_string())?;
        notify.signal();
        reply_rx
            .recv()
            .map_err(|_| "dump reply channel dropped (node shut down?)".to_string())?
    }
}

impl NodeHandle {
    pub fn dumper(&self) -> NodeDumper {
        NodeDumper {
            shards: self
                .shards
                .iter()
                .map(|s| (s.ctl.clone(), s.ctl_notify.clone()))
                .collect(),
        }
    }

    pub fn set_sweep_every(&self, sweep_every: Duration) {
        for s in &self.shards {
            if s.ctl.send(NodeCtl::SetSweepEvery(sweep_every)).is_ok() {
                s.ctl_notify.signal();
            }
        }
    }

    /// Stops the node: signals every shard, which stops accepting, makes a
    /// bounded attempt to finish in-flight requests, syncs its stores and
    /// persists its liveness state, then joins the shards and maintenance
    /// threads. Errors are logged, not returned, so do not treat a clean
    /// return as proof that every buffered write reached the device; use
    /// device-durable writes for that. Dropping the handle instead leaves the
    /// threads running.
    pub fn shutdown(mut self) {
        let start = std::time::Instant::now();
        for s in &self.shards {
            s.stop.signal();
        }
        for s in self.shards {
            let _ = s.join.join();
        }
        log::info!("node shutdown: shards joined in {:?}", start.elapsed());
        self.maint_join.cancel();
        let maint_start = std::time::Instant::now();
        self.maint_join.join();
        let (dropped, cancelled) = self.maint_join.cancellation_counts();
        log::info!(
            "node shutdown: maintenance joined in {:?}, total {:?}; {dropped} jobs dropped, {cancelled} merges cancelled",
            maint_start.elapsed(), start.elapsed()
        );
    }

    pub fn rebalance_drives<DS>(
        &self,
        ds: &DS,
        new_caps: &[u64],
        geom: crate::drive::PartitionGeom,
        node_id: NodeId,
    ) -> Result<(usize, u32), crate::drive::LayoutError>
    where
        DS: crate::drive::DriveSet,
        DS::Dev: BlockDevice + Send + 'static,
    {
        let shards: Vec<(mpsc::Sender<NodeCtl>, Arc<crate::reactor::Notify>)> = self
            .shards
            .iter()
            .map(|s| (s.ctl.clone(), s.ctl_notify.clone()))
            .collect();
        rebalance_shards(&shards, ds, new_caps, geom, node_id)
    }

    pub fn rebalancer(&self) -> NodeRebalancer {
        NodeRebalancer {
            shards: self
                .shards
                .iter()
                .map(|s| (s.ctl.clone(), s.ctl_notify.clone()))
                .collect(),
        }
    }
}

#[derive(Clone)]
pub struct NodeRebalancer {
    shards: Vec<(mpsc::Sender<NodeCtl>, Arc<crate::reactor::Notify>)>,
}

impl NodeRebalancer {
    pub fn rebalance_drives<DS>(
        &self,
        ds: &DS,
        new_caps: &[u64],
        geom: crate::drive::PartitionGeom,
        node_id: NodeId,
    ) -> Result<(usize, u32), crate::drive::LayoutError>
    where
        DS: crate::drive::DriveSet,
        DS::Dev: BlockDevice + Send + 'static,
    {
        rebalance_shards(&self.shards, ds, new_caps, geom, node_id)
    }
}

fn rebalance_shards<DS>(
    shards: &[(mpsc::Sender<NodeCtl>, Arc<crate::reactor::Notify>)],
    ds: &DS,
    new_caps: &[u64],
    geom: crate::drive::PartitionGeom,
    node_id: NodeId,
) -> Result<(usize, u32), crate::drive::LayoutError>
where
    DS: crate::drive::DriveSet,
    DS::Dev: BlockDevice + Send + 'static,
{
    use crate::drive::{node_weight, relocate_with, replan, Layout, LayoutError};
    let shard_count = shards.len();
    let fp = ds.read_layout_fingerprint()?.ok_or(LayoutError::Corrupt(
        "live rebalance requires a persisted layout fingerprint",
    ))?;
    let old = Layout::decode(&fp).ok_or(LayoutError::Corrupt(
        "persisted layout fingerprint failed to decode",
    ))?;
    if old.num_partitions() != shard_count {
        return Err(LayoutError::Corrupt(
            "persisted layout partition count != node shard count",
        ));
    }
    let new = replan(&old, new_caps, shard_count)?;

    let moved = relocate_with(ds, &old, &new, |src, dst, commit| {
        let partition = if dst.partition != usize::MAX {
            dst.partition
        } else {
            src.partition
        };
        let dev = ds
            .open_region(dst.drive, dst.base, dst.len, true)
            .map_err(LayoutError::Io)?;
        let (ctl, notify) = shards.get(partition).ok_or(LayoutError::Corrupt(
            "relocate move references a partition beyond the node's shards",
        ))?;
        let (prepared_tx, prepared_rx) = mpsc::channel();
        let (commit_tx, commit_rx) = mpsc::channel();
        let (reply_tx, reply_rx) = mpsc::channel();
        ctl.send(NodeCtl::SwapStore {
            dev: Box::new(dev),
            geom,
            node: node_id,
            prepared: prepared_tx,
            commit: commit_rx,
            reply: reply_tx,
        })
        .map_err(|_| LayoutError::Io(io::Error::other("shard is gone (shutting down?)")))?;
        notify.signal();
        match prepared_rx.recv() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(LayoutError::Io(io::Error::other(e))),
            Err(_) => {
                return Err(LayoutError::Io(io::Error::other(
                    "swap prepared channel dropped (shard shut down?)",
                )))
            }
        }
        if let Err(e) = commit() {
            drop(commit_tx);
            let _ = reply_rx.recv();
            return Err(e);
        }
        if commit_tx.send(()).is_err() {
            return Err(LayoutError::Io(io::Error::other(
                "swap commit channel dropped (shard shut down?)",
            )));
        }
        match reply_rx.recv() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(LayoutError::Io(io::Error::other(e))),
            Err(_) => Err(LayoutError::Io(io::Error::other(
                "swap reply channel dropped (shard shut down?)",
            ))),
        }
    })?;
    let new_drives = new_caps.iter().filter(|&&c| c > 0).count();
    Ok((moved, node_weight(new_drives)))
}

