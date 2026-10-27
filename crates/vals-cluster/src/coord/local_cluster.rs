use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};

use vals_spdk::BlockDevice;

use crate::hlc::Version;
use crate::replica::{Applied, Stored, VersionedStore};
use crate::ring::NodeId;
use crate::zone::{Catalog, ZoneId, DEFAULT_ZONE};

use super::{BatchAck, BatchEntry, RingView, ScanPage, Transport, TransportError};

pub struct LocalCluster<D: BlockDevice> {
    nodes: HashMap<(NodeId, ZoneId), RefCell<VersionedStore<D>>>,
    down: RefCell<HashSet<NodeId>>,
    catalog: Catalog,
    scan_page_entries: Cell<usize>,
}

impl<D: BlockDevice> Default for LocalCluster<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D: BlockDevice> LocalCluster<D> {
    pub fn new() -> Self {
        LocalCluster {
            nodes: HashMap::new(),
            down: RefCell::new(HashSet::new()),
            catalog: Catalog::new(),
            scan_page_entries: Cell::new(usize::MAX),
        }
    }

    pub fn set_scan_page_entries(&self, max: usize) {
        self.scan_page_entries.set(max.max(1));
    }

    pub fn insert(&mut self, node: NodeId, store: VersionedStore<D>) {
        self.insert_zone(node, DEFAULT_ZONE, store);
    }

    pub fn insert_zone(&mut self, node: NodeId, zone_id: ZoneId, store: VersionedStore<D>) {
        self.nodes.insert((node, zone_id), RefCell::new(store));
    }

    pub fn define_zone(&mut self, zone_id: ZoneId, cfg: crate::zone::ZoneConfig) {
        self.catalog.insert(zone_id, cfg);
    }

    pub fn set_down(&self, node: NodeId, down: bool) {
        if down {
            self.down.borrow_mut().insert(node);
        } else {
            self.down.borrow_mut().remove(&node);
        }
    }

    pub fn store(&self, node: NodeId) -> Option<&RefCell<VersionedStore<D>>> {
        self.store_zone(node, DEFAULT_ZONE)
    }

    pub fn store_zone(&self, node: NodeId, zone_id: ZoneId) -> Option<&RefCell<VersionedStore<D>>> {
        self.nodes.get(&(node, zone_id))
    }

    fn check_up(&self, node: NodeId) -> Result<(), TransportError> {
        if self.down.borrow().contains(&node) {
            Err(TransportError::Unavailable)
        } else {
            Ok(())
        }
    }
}

impl<D: BlockDevice> Transport for LocalCluster<D> {
    async fn apply(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
    ) -> Result<Applied, TransportError> {
        self.check_up(node)?;
        let store = self
            .nodes
            .get(&(node, zone_id))
            .ok_or(TransportError::Unavailable)?;
        store
            .borrow_mut()
            .apply(key, value, version)
            .map_err(|e| {
                if e.is_backpressure() {
                    TransportError::Busy
                } else {
                    TransportError::Engine(e.to_string())
                }
            })
    }

    async fn read(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Option<Stored>, TransportError> {
        self.check_up(node)?;
        let store = self
            .nodes
            .get(&(node, zone_id))
            .ok_or(TransportError::Unavailable)?;
        store
            .borrow()
            .get_versioned(key)
            .map_err(|e| TransportError::Engine(e.to_string()))
    }

    async fn scan(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max: u32,
    ) -> Result<ScanPage, TransportError> {
        self.check_up(node)?;
        let store = self
            .nodes
            .get(&(node, zone_id))
            .ok_or(TransportError::Unavailable)?;
        let cap = if max == 0 { usize::MAX } else { max as usize };
        let store = store.borrow();
        let (entries, more) = store
            .scan_page(lo, hi, cap.min(self.scan_page_entries.get()), usize::MAX)
            .map_err(|e| TransportError::Engine(e.to_string()))?;
        let set = store.engine().range_tombstones();
        let fragments = if set.is_empty() {
            Vec::new()
        } else {
            let end = match entries.last() {
                Some((last, _)) if more => Some([last.as_slice(), &[0]].concat()),
                _ => hi.map(<[u8]>::to_vec),
            };
            set.clipped(lo.unwrap_or_default(), end.as_deref())
                .into_iter()
                .map(|f| crate::wire::RtFragment {
                    lo: (!f.lo.is_empty()).then_some(f.lo),
                    hi: f.hi,
                    version: crate::replica::unpack_version(f.version),
                })
                .collect()
        };
        Ok(ScanPage { entries, more, fragments, resume: None })
    }

    fn supports_range_tombstones(&self, _node: NodeId) -> Option<bool> {
        Some(true)
    }

    async fn range_delete(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        sync: bool,
        version: Version,
        slices: &[crate::wire::WireSlice<'_>],
    ) -> Vec<BatchAck> {
        if self.check_up(node).is_err() {
            return vec![BatchAck::Unreachable; slices.len()];
        }
        let Some(store) = self.nodes.get(&(node, zone_id)) else {
            return vec![BatchAck::Unreachable; slices.len()];
        };
        let mut store = store.borrow_mut();
        let wall_ms = crate::hlc::physical_now_ms();
        let out: Vec<BatchAck> = slices
            .iter()
            .map(|&(lo, hi)| match store.apply_range_delete(lo, hi, version, wall_ms) {
                Ok(a) => BatchAck::Applied(a),
                Err(ref err) if err.is_backpressure() => BatchAck::Busy,
                Err(_) => BatchAck::Remote,
            })
            .collect();
        if sync {
            if let Err(e) = store.sync() {
                if e.is_backpressure() {
                    return vec![BatchAck::Busy; slices.len()];
                }
                return vec![BatchAck::Unreachable; slices.len()];
            }
        }
        out
    }

    async fn members(&self, node: NodeId) -> Result<RingView, TransportError> {
        self.check_up(node)?;
        let members: std::collections::BTreeSet<NodeId> =
            self.nodes.keys().map(|&(n, _)| n).collect();
        Ok(RingView {
            epoch: 0,
            vnodes: 0,
            rf: 0,
            members: members.into_iter().map(|n| (n, 1)).collect(),
            addrs: Vec::new(),
        })
    }

    async fn zones(&self, node: NodeId) -> Result<Catalog, TransportError> {
        self.check_up(node)?;
        Ok(self.catalog.clone())
    }

    async fn apply_batch(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        sync: bool,
        writes: &[BatchEntry<'_>],
    ) -> Vec<BatchAck> {
        if self.check_up(node).is_err() {
            return vec![BatchAck::Unreachable; writes.len()];
        }
        let Some(store) = self.nodes.get(&(node, zone_id)) else {
            return vec![BatchAck::Unreachable; writes.len()];
        };
        let mut store = store.borrow_mut();
        let out: Vec<BatchAck> = writes
            .iter()
            .map(|e| match store.apply(e.key, e.value, e.version) {
                Ok(a) => BatchAck::Applied(a),
                Err(ref err) if err.is_backpressure() => BatchAck::Busy,
                Err(_) => BatchAck::Unreachable,
            })
            .collect();
        if sync {
            if let Err(e) = store.sync() {
                if e.is_backpressure() {
                    return vec![BatchAck::Busy; writes.len()];
                }
                return vec![BatchAck::Unreachable; writes.len()];
            }
        }
        out
    }
}
