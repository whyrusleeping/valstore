#[cfg(test)]
use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use vals_spdk::{
    AsyncRead, BlockDevice, BlockError, FileBlockDevice, MaintenanceCacheKey, MemBlockDevice,
    ReadOp, Result as BlockResult, Scratch, WriteHandle,
};
use vals_store::crc::crc32;
use vals_store::{Config, Engine, EngineError};

use crate::liveness::{LivenessLog, WipeStats};
use crate::replica::VersionedStore;
use crate::zone::ZoneId;

#[derive(Debug)]
pub enum ZoneStoreError {
    Io(std::io::Error),
    Block(BlockError),
    Engine(EngineError),
    Exhausted {
        capacity_zones: usize,
    },
    Corrupt(&'static str),
    Mismatch(String),
}

impl fmt::Display for ZoneStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ZoneStoreError::Io(e) => write!(f, "{e}"),
            ZoneStoreError::Block(e) => write!(f, "{e}"),
            ZoneStoreError::Engine(e) => write!(f, "{e}"),
            ZoneStoreError::Exhausted { capacity_zones } => {
                write!(
                    f,
                    "namespace full: no contiguous extent fits \
                     (fallback-equivalent capacity {capacity_zones} zones)"
                )
            }
            ZoneStoreError::Corrupt(what) => write!(f, "corrupt zone metadata: {what}"),
            ZoneStoreError::Mismatch(what) => write!(f, "{what}"),
        }
    }
}

impl std::error::Error for ZoneStoreError {}

impl From<std::io::Error> for ZoneStoreError {
    fn from(e: std::io::Error) -> Self {
        ZoneStoreError::Io(e)
    }
}
impl From<BlockError> for ZoneStoreError {
    fn from(e: BlockError) -> Self {
        ZoneStoreError::Block(e)
    }
}
impl From<EngineError> for ZoneStoreError {
    fn from(e: EngineError) -> Self {
        ZoneStoreError::Engine(e)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ZoneGeom {
    pub block_size: usize,
    pub seg_blocks: u64,
    pub reserved_blocks: u64,
    pub region_blocks: u64,
}

impl ZoneGeom {
    fn config(&self, ordering: Ordering) -> Config {
        Config {
            seg_blocks: self.seg_blocks,
            reserved_blocks: self.reserved_blocks,
            ordering: engine_ordering(ordering),
        }
    }
}

pub trait ZoneStore<D: BlockDevice> {
    fn open(
        &self,
        zone_id: ZoneId,
        ordering: Ordering,
    ) -> Result<VersionedStore<D>, ZoneStoreError>;

    fn drop_zone(&self, zone_id: ZoneId) -> Result<(), ZoneStoreError>;

    fn existing(&self) -> Vec<ZoneId>;

    fn zone_ordering(&self, _zone_id: ZoneId) -> Option<Ordering> {
        None
    }

    fn persisted_incarnation(&self) -> Option<u128> {
        None
    }

    fn persist_incarnation(&self, _incarnation: Option<u128>) -> Result<(), ZoneStoreError> {
        Ok(())
    }
}

pub struct MemZoneStore {
    node_id: u32,
    geom: ZoneGeom,
    devices: RefCell<HashMap<ZoneId, MemBlockDevice>>,
    orderings: RefCell<HashMap<ZoneId, Ordering>>,
}

impl MemZoneStore {
    pub fn new(node_id: u32, geom: ZoneGeom) -> Self {
        MemZoneStore {
            node_id,
            geom,
            devices: RefCell::new(HashMap::new()),
            orderings: RefCell::new(HashMap::new()),
        }
    }
}

impl ZoneStore<MemBlockDevice> for MemZoneStore {
    fn open(
        &self,
        zone_id: ZoneId,
        ordering: Ordering,
    ) -> Result<VersionedStore<MemBlockDevice>, ZoneStoreError> {
        let mut devices = self.devices.borrow_mut();
        let cfg = self.geom.config(ordering);
        let store = match devices.get(&zone_id) {
            Some(dev) => VersionedStore::open(dev.share(), cfg, self.node_id)?,
            None => {
                let dev = MemBlockDevice::new(self.geom.block_size, self.geom.region_blocks);
                let store =
                    VersionedStore::new(Engine::create_prewiped(dev.share(), cfg), self.node_id)?;
                devices.insert(zone_id, dev);
                store
            }
        };
        self.orderings.borrow_mut().insert(zone_id, ordering);
        Ok(store)
    }

    fn drop_zone(&self, zone_id: ZoneId) -> Result<(), ZoneStoreError> {
        self.devices.borrow_mut().remove(&zone_id);
        self.orderings.borrow_mut().remove(&zone_id);
        Ok(())
    }

    fn existing(&self) -> Vec<ZoneId> {
        let mut ids: Vec<ZoneId> = self.devices.borrow().keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    fn zone_ordering(&self, zone_id: ZoneId) -> Option<Ordering> {
        self.orderings.borrow().get(&zone_id).copied()
    }
}

pub struct FileZoneStore {
    node_id: u32,
    dir: PathBuf,
    geom: ZoneGeom,
    incarnation: std::cell::Cell<Option<u128>>,
}

const INCARNATION_MAGIC: u32 = 0x5649_4E43;
const INCARNATION_VERSION: u16 = 1;
const INCARNATION_FILE_BYTES: usize = 4 + 2 + 16 + 4;

fn encode_incarnation_file(id: u128) -> Vec<u8> {
    let mut b = Vec::with_capacity(INCARNATION_FILE_BYTES);
    b.extend_from_slice(&INCARNATION_MAGIC.to_le_bytes());
    b.extend_from_slice(&INCARNATION_VERSION.to_le_bytes());
    b.extend_from_slice(&id.to_le_bytes());
    let crc = crc32(&b);
    b.extend_from_slice(&crc.to_le_bytes());
    b
}

fn decode_incarnation_file(b: &[u8]) -> Result<u128, ZoneStoreError> {
    if b.len() != INCARNATION_FILE_BYTES
        || u32::from_le_bytes(b[0..4].try_into().unwrap()) != INCARNATION_MAGIC
        || crc32(&b[..22]) != u32::from_le_bytes(b[22..26].try_into().unwrap())
    {
        return Err(ZoneStoreError::Corrupt(
            "cluster incarnation file is damaged; rewrite it offline with `valsd reenrol`",
        ));
    }
    if u16::from_le_bytes(b[4..6].try_into().unwrap()) != INCARNATION_VERSION {
        return Err(ZoneStoreError::Corrupt(
            "cluster incarnation file has an unsupported version",
        ));
    }
    Ok(u128::from_le_bytes(b[6..22].try_into().unwrap()))
}

impl FileZoneStore {
    pub fn new(
        node_id: u32,
        dir: impl Into<PathBuf>,
        geom: ZoneGeom,
    ) -> Result<Self, ZoneStoreError> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        let incarnation = match std::fs::read(dir.join("incarnation")) {
            Ok(bytes) => Some(decode_incarnation_file(&bytes)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        Ok(FileZoneStore {
            node_id,
            dir,
            geom,
            incarnation: std::cell::Cell::new(incarnation),
        })
    }

    fn zone_path(&self, zone_id: ZoneId) -> PathBuf {
        self.dir.join(format!("zone-{zone_id}.vlog"))
    }
}

impl ZoneStore<FileBlockDevice> for FileZoneStore {
    fn open(
        &self,
        zone_id: ZoneId,
        ordering: Ordering,
    ) -> Result<VersionedStore<FileBlockDevice>, ZoneStoreError> {
        let path = self.zone_path(zone_id);
        let cfg = self.geom.config(ordering);
        let store = if path.exists() {
            let dev = FileBlockDevice::open(&path, self.geom.block_size)?;
            VersionedStore::open(dev, cfg, self.node_id)?
        } else {
            let dev =
                FileBlockDevice::create(&path, self.geom.block_size, self.geom.region_blocks)?;
            VersionedStore::new(Engine::create_prewiped(dev, cfg), self.node_id)?
        };
        Ok(store)
    }

    fn drop_zone(&self, zone_id: ZoneId) -> Result<(), ZoneStoreError> {
        let path = self.zone_path(zone_id);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn existing(&self) -> Vec<ZoneId> {
        let mut ids = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if let Some(id) = name
                        .strip_prefix("zone-")
                        .and_then(|s| s.strip_suffix(".vlog"))
                        .and_then(|s| s.parse::<ZoneId>().ok())
                    {
                        ids.push(id);
                    }
                }
            }
        }
        ids.sort_unstable();
        ids
    }

    fn persisted_incarnation(&self) -> Option<u128> {
        self.incarnation.get()
    }

    fn persist_incarnation(&self, incarnation: Option<u128>) -> Result<(), ZoneStoreError> {
        let path = self.dir.join("incarnation");
        match incarnation {
            Some(id) => {
                use std::io::Write;
                let tmp = self.dir.join("incarnation.tmp");
                let mut f = std::fs::File::create(&tmp)?;
                f.write_all(&encode_incarnation_file(id))?;
                f.sync_all()?;
                std::fs::rename(&tmp, &path)?;
            }
            None => match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            },
        }
        std::fs::File::open(&self.dir)?.sync_all()?;
        self.incarnation.set(incarnation);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZoneRegion {
    pub zone_id: ZoneId,
    pub base: u64,
    pub len: u64,
    pub ordering: Ordering,
}

const SUPER_MAGIC: u32 = 0x564c_5a53;
const SUPER_VERSION: u16 = 4;
const SUPER_VERSION_UNBOUND: u16 = 3;

use crate::zone::{ordering_from_u8, ordering_to_u8, Ordering};

pub(crate) fn engine_ordering(o: Ordering) -> vals_store::Ordering {
    match o {
        Ordering::Sorted => vals_store::Ordering::Sorted,
        Ordering::Unsorted => vals_store::Ordering::Unsorted,
    }
}

const REGION_ENTRY_BYTES: usize = 4 + 8 + 8 + 1;
const SUPER_HEADER_V1_BYTES: usize = 4 + 2 + 8 + 4;
const SUPER_HEADER_BYTES: usize = 4 + 2 + 8 + 1 + 8 + 4;
const TOPOLOGY_BYTES: usize = 8 + 4 + 1 + 4 + 4 + 4 + 8 + 8 + 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZoneTopology {
    pub layout_id: u64,
    pub node_id: u32,
    pub kind: u8,
    pub partition_count: u32,
    pub partition_index: u32,
    pub drive: u32,
    pub base: u64,
    pub len: u64,
    pub geometry: u64,
}

impl ZoneTopology {
    pub fn single(node_id: u32, len: u64, cfg: &RegionZoneConfig, block_size: usize) -> Self {
        Self {
            layout_id: fresh_layout_id(), node_id, kind: 0, partition_count: 1,
            partition_index: 0, drive: 0, base: 0, len,
            geometry: cfg.geometry_fingerprint(block_size),
        }
    }

    fn partition(
        layout_id: u64, node_id: u32, count: usize, p: &crate::drive::Partition,
        cfg: &RegionZoneConfig, block_size: usize,
    ) -> Self {
        Self {
            layout_id, node_id, kind: 1, partition_count: count as u32,
            partition_index: p.partition as u32, drive: p.drive as u32,
            base: p.base, len: p.len, geometry: cfg.geometry_fingerprint(block_size),
        }
    }

    pub fn verify(self, expected: Self) -> Result<(), ZoneStoreError> {
        let actual = Self { layout_id: expected.layout_id, ..self };
        if actual != expected {
            return Err(ZoneStoreError::Mismatch(format!(
                "zone topology/geometry mismatch: stored {self:?}, expected {expected:?}; \
                 run `valsd provision` while the node is offline"
            )));
        }
        Ok(())
    }
}

fn fresh_layout_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap();
    let mut bytes = now.as_nanos().to_le_bytes().to_vec();
    bytes.extend_from_slice(&std::process::id().to_le_bytes());
    bytes.extend_from_slice(&NEXT.fetch_add(1, AtomicOrdering::Relaxed).to_le_bytes());
    crate::ring::hash64(&bytes)
}

type SuperRecord = (u64, Vec<ZoneRegion>, Option<u64>, Option<ZoneTopology>, Option<u128>);

fn encode_superblock(
    generation: u64,
    regions: &[ZoneRegion],
    watermark: Option<u64>,
    topology: Option<ZoneTopology>,
    incarnation: Option<u128>,
) -> Vec<u8> {
    let header_bytes = SUPER_HEADER_BYTES + 1 + if topology.is_some() { TOPOLOGY_BYTES } else { 0 } + 17;
    let version = if incarnation.is_some() { SUPER_VERSION } else { SUPER_VERSION_UNBOUND };
    let mut buf = Vec::with_capacity(header_bytes + regions.len() * REGION_ENTRY_BYTES + 4);
    buf.extend_from_slice(&SUPER_MAGIC.to_le_bytes());
    buf.extend_from_slice(&version.to_le_bytes());
    buf.extend_from_slice(&generation.to_le_bytes());
    buf.push(u8::from(watermark.is_some()));
    buf.extend_from_slice(&watermark.unwrap_or(0).to_le_bytes());
    buf.extend_from_slice(&(regions.len() as u32).to_le_bytes());
    buf.push(u8::from(topology.is_some()));
    if let Some(t) = topology {
        buf.extend_from_slice(&t.layout_id.to_le_bytes());
        buf.extend_from_slice(&t.node_id.to_le_bytes());
        buf.push(t.kind);
        buf.extend_from_slice(&t.partition_count.to_le_bytes());
        buf.extend_from_slice(&t.partition_index.to_le_bytes());
        buf.extend_from_slice(&t.drive.to_le_bytes());
        buf.extend_from_slice(&t.base.to_le_bytes());
        buf.extend_from_slice(&t.len.to_le_bytes());
        buf.extend_from_slice(&t.geometry.to_le_bytes());
    }
    if let Some(id) = incarnation {
        buf.push(1);
        buf.extend_from_slice(&id.to_le_bytes());
    }
    for r in regions {
        buf.extend_from_slice(&r.zone_id.to_le_bytes());
        buf.extend_from_slice(&r.base.to_le_bytes());
        buf.extend_from_slice(&r.len.to_le_bytes());
        buf.push(ordering_to_u8(r.ordering));
    }
    let crc = crc32(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

fn decode_regions(buf: &[u8], mut off: usize, count: usize) -> Option<Vec<ZoneRegion>> {
    let mut regions = Vec::with_capacity(count);
    for _ in 0..count {
        let zone_id = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap());
        let base = u64::from_le_bytes(buf[off + 4..off + 12].try_into().unwrap());
        let len = u64::from_le_bytes(buf[off + 12..off + 20].try_into().unwrap());
        let ordering = ordering_from_u8(buf[off + 20])?;
        regions.push(ZoneRegion {
            zone_id,
            base,
            len,
            ordering,
        });
        off += REGION_ENTRY_BYTES;
    }
    Some(regions)
}

fn decode_superblock(buf: &[u8]) -> Result<Option<SuperRecord>, ZoneStoreError> {
    if buf.len() < 6 {
        return Ok(None);
    }
    let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    if magic != SUPER_MAGIC {
        return Ok(None);
    }
    let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
    match version {
        1 => decode_superblock_v1(buf),
        2..=4 => decode_superblock_v2_to_v4(buf),
        _ => Err(ZoneStoreError::Corrupt(
            "unsupported superblock format version; run `valsd provision` while the node is offline",
        )),
    }
}

fn decode_superblock_v1(buf: &[u8]) -> Result<Option<SuperRecord>, ZoneStoreError> {
    if buf.len() < SUPER_HEADER_V1_BYTES {
        return Ok(None);
    }
    let generation = u64::from_le_bytes(buf[6..14].try_into().unwrap());
    let count = u32::from_le_bytes(buf[14..18].try_into().unwrap()) as usize;
    let body_end = SUPER_HEADER_V1_BYTES + count * REGION_ENTRY_BYTES;
    if buf.len() < body_end + 4 {
        return Ok(None);
    }
    let crc_stored = u32::from_le_bytes(buf[body_end..body_end + 4].try_into().unwrap());
    if crc32(&buf[..body_end]) != crc_stored {
        return Ok(None);
    }
    match decode_regions(buf, SUPER_HEADER_V1_BYTES, count) {
        Some(regions) => Ok(Some((generation, regions, None, None, None))),
        None => Ok(None),
    }
}

fn decode_superblock_v2_to_v4(buf: &[u8]) -> Result<Option<SuperRecord>, ZoneStoreError> {
    if buf.len() < SUPER_HEADER_BYTES {
        return Ok(None);
    }
    let generation = u64::from_le_bytes(buf[6..14].try_into().unwrap());
    let wm_present = buf[14] != 0;
    let wm = u64::from_le_bytes(buf[15..23].try_into().unwrap());
    let count = u32::from_le_bytes(buf[23..27].try_into().unwrap()) as usize;
    let mut header_end = SUPER_HEADER_BYTES;
    let mut topology = None;
    let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
    if version >= 3 {
        let Some(&present) = buf.get(header_end) else { return Ok(None); };
        header_end += 1;
        match present {
            0 => {}
            1 => {
                if buf.len() < header_end + TOPOLOGY_BYTES { return Ok(None); }
                let t = &buf[header_end..header_end + TOPOLOGY_BYTES];
                topology = Some(ZoneTopology {
                    layout_id: u64::from_le_bytes(t[0..8].try_into().unwrap()),
                    node_id: u32::from_le_bytes(t[8..12].try_into().unwrap()),
                    kind: t[12],
                    partition_count: u32::from_le_bytes(t[13..17].try_into().unwrap()),
                    partition_index: u32::from_le_bytes(t[17..21].try_into().unwrap()),
                    drive: u32::from_le_bytes(t[21..25].try_into().unwrap()),
                    base: u64::from_le_bytes(t[25..33].try_into().unwrap()),
                    len: u64::from_le_bytes(t[33..41].try_into().unwrap()),
                    geometry: u64::from_le_bytes(t[41..49].try_into().unwrap()),
                });
                header_end += TOPOLOGY_BYTES;
            }
            _ => return Ok(None),
        }
    }
    let mut incarnation = None;
    if version >= 4 {
        let Some(&present) = buf.get(header_end) else { return Ok(None); };
        header_end += 1;
        match present {
            0 => {}
            1 => {
                let Some(id) = buf.get(header_end..header_end + 16) else { return Ok(None); };
                incarnation = Some(u128::from_le_bytes(id.try_into().unwrap()));
                header_end += 16;
            }
            _ => return Ok(None),
        }
    }
    let body_end = header_end + count * REGION_ENTRY_BYTES;
    if buf.len() < body_end + 4 {
        return Ok(None);
    }
    let crc_stored = u32::from_le_bytes(buf[body_end..body_end + 4].try_into().unwrap());
    if crc32(&buf[..body_end]) != crc_stored {
        return Ok(None);
    }
    match decode_regions(buf, header_end, count) {
        Some(regions) => Ok(Some((
            generation,
            regions,
            wm_present.then_some(wm),
            topology,
            incarnation,
        ))),
        None => Ok(None),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FreeExtent {
    base: u64,
    len: u64,
}

#[derive(Debug, Clone)]
pub struct RegionTable {
    super_blocks: u64,
    region_blocks: u64,
    total_blocks: u64,
    overrides: HashMap<ZoneId, u64>,
    regions: HashMap<ZoneId, ZoneRegion>,
    free: Vec<FreeExtent>,
}

impl RegionTable {
    pub fn new(super_blocks: u64, region_blocks: u64, total_blocks: u64) -> Self {
        Self::new_variable(
            super_blocks,
            region_blocks,
            total_blocks,
            region_blocks,
            region_blocks,
            &[],
        )
        .expect("fixed region geometry must be valid")
    }

    pub fn from_regions(
        super_blocks: u64,
        region_blocks: u64,
        total_blocks: u64,
        recovered: Vec<ZoneRegion>,
    ) -> Result<Self, ZoneStoreError> {
        if recovered.iter().any(|region| region.len != region_blocks) {
            return Err(ZoneStoreError::Corrupt(
                "recovered fixed region length differs from configured region_blocks",
            ));
        }
        Self::from_regions_variable(
            super_blocks,
            region_blocks,
            total_blocks,
            region_blocks,
            region_blocks,
            &[],
            recovered,
        )
    }

    pub fn new_variable(
        super_blocks: u64,
        region_blocks: u64,
        total_blocks: u64,
        alignment_blocks: u64,
        min_region_blocks: u64,
        overrides: &[(ZoneId, u64)],
    ) -> Result<Self, ZoneStoreError> {
        Self::from_regions_variable(
            super_blocks,
            region_blocks,
            total_blocks,
            alignment_blocks,
            min_region_blocks,
            overrides,
            Vec::new(),
        )
    }

    pub fn from_regions_variable(
        super_blocks: u64,
        region_blocks: u64,
        total_blocks: u64,
        alignment_blocks: u64,
        min_region_blocks: u64,
        overrides: &[(ZoneId, u64)],
        recovered: Vec<ZoneRegion>,
    ) -> Result<Self, ZoneStoreError> {
        if alignment_blocks == 0
            || region_blocks < min_region_blocks
            || !region_blocks.is_multiple_of(alignment_blocks)
            || super_blocks > total_blocks
            || super_blocks
                .checked_add(region_blocks)
                .is_none_or(|end| end > total_blocks)
        {
            return Err(ZoneStoreError::Corrupt(
                "configured zone geometry is zero, undersized, misaligned, or out of bounds",
            ));
        }
        let mut override_map = HashMap::new();
        for &(zone_id, len) in overrides {
            if len < min_region_blocks
                || !len.is_multiple_of(alignment_blocks)
                || super_blocks
                    .checked_add(len)
                    .is_none_or(|end| end > total_blocks)
            {
                return Err(ZoneStoreError::Corrupt(
                    "configured zone override is undersized, segment-misaligned, or out of bounds",
                ));
            }
            if override_map.insert(zone_id, len).is_some() {
                return Err(ZoneStoreError::Corrupt(
                    "configured zone overrides contain a duplicate zone id",
                ));
            }
        }

        let mut sorted = recovered;
        sorted.sort_unstable_by_key(|r| r.base);
        let mut regions = HashMap::new();
        let mut previous_end = super_blocks;
        let mut free = Vec::new();
        for r in &sorted {
            let end = r.base.checked_add(r.len).ok_or(ZoneStoreError::Corrupt(
                "recovered region end overflows",
            ))?;
            if r.base < super_blocks
                || !(r.base - super_blocks).is_multiple_of(alignment_blocks)
                || r.len < min_region_blocks
                || !r.len.is_multiple_of(alignment_blocks)
                || end > total_blocks
            {
                return Err(ZoneStoreError::Corrupt(
                    "recovered region is undersized, misaligned, or out of bounds",
                ));
            }
            if r.base < previous_end {
                return Err(ZoneStoreError::Corrupt("recovered regions overlap"));
            }
            if regions.insert(r.zone_id, *r).is_some() {
                return Err(ZoneStoreError::Corrupt(
                    "recovered regions contain a duplicate zone id",
                ));
            }
            if r.base > previous_end {
                free.push(FreeExtent {
                    base: previous_end,
                    len: r.base - previous_end,
                });
            }
            previous_end = end;
        }
        if previous_end < total_blocks {
            free.push(FreeExtent {
                base: previous_end,
                len: total_blocks - previous_end,
            });
        }
        Ok(RegionTable {
            super_blocks,
            region_blocks,
            total_blocks,
            overrides: override_map,
            regions,
            free,
        })
    }

    pub fn get(&self, zone_id: ZoneId) -> Option<ZoneRegion> {
        self.regions.get(&zone_id).copied()
    }

    pub fn alloc(
        &mut self,
        zone_id: ZoneId,
        ordering: Ordering,
    ) -> Result<ZoneRegion, ZoneStoreError> {
        if let Some(r) = self.regions.get(&zone_id) {
            return Ok(*r);
        }
        let len = self
            .overrides
            .get(&zone_id)
            .copied()
            .unwrap_or(self.region_blocks);
        let Some(slot) = self.free.iter().position(|extent| extent.len >= len) else {
            return Err(ZoneStoreError::Exhausted {
                capacity_zones: self.capacity(),
            });
        };
        let base = self.free[slot].base;
        self.free[slot].base += len;
        self.free[slot].len -= len;
        if self.free[slot].len == 0 {
            self.free.remove(slot);
        }
        let region = ZoneRegion {
            zone_id,
            base,
            len,
            ordering,
        };
        self.regions.insert(zone_id, region);
        Ok(region)
    }

    pub fn free_zone(&mut self, zone_id: ZoneId) -> Option<ZoneRegion> {
        let r = self.regions.remove(&zone_id)?;
        let mut slot = self
            .free
            .binary_search_by_key(&r.base, |extent| extent.base)
            .unwrap_or_else(|slot| slot);
        self.free.insert(
            slot,
            FreeExtent {
                base: r.base,
                len: r.len,
            },
        );
        if slot > 0 {
            let left_end = self.free[slot - 1].base + self.free[slot - 1].len;
            if left_end == self.free[slot].base {
                let len = self.free[slot].len;
                self.free[slot - 1].len += len;
                self.free.remove(slot);
                slot -= 1;
            }
        }
        if slot + 1 < self.free.len() {
            let end = self.free[slot].base + self.free[slot].len;
            if end == self.free[slot + 1].base {
                let len = self.free[slot + 1].len;
                self.free[slot].len += len;
                self.free.remove(slot + 1);
            }
        }
        Some(r)
    }

    pub fn regions(&self) -> Vec<ZoneRegion> {
        let mut v: Vec<ZoneRegion> = self.regions.values().copied().collect();
        v.sort_unstable_by_key(|r| r.base);
        v
    }

    pub fn zone_ids(&self) -> Vec<ZoneId> {
        let mut ids: Vec<ZoneId> = self.regions.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    pub fn capacity(&self) -> usize {
        if self.region_blocks == 0 {
            return 0;
        }
        ((self.total_blocks.saturating_sub(self.super_blocks)) / self.region_blocks) as usize
    }
}

fn wipe_region<D: BlockDevice>(dev: &D, cfg: &Config) -> Result<(), ZoneStoreError> {
    const MAX_INFLIGHT_WIPES: usize = 256;

    let block_size = dev.block_size();
    let num_segments = dev.num_blocks().saturating_sub(cfg.reserved_blocks) / cfg.seg_blocks;
    if num_segments >= 1 << 16 {
        log::info!(
            "zonestore: wiping region ({num_segments} segment headers, \
             {seg_blocks}-block segments) before first use",
            seg_blocks = cfg.seg_blocks,
        );
    }
    let zero = vec![0u8; block_size];

    let mut inflight: Vec<WriteHandle> = Vec::new();
    let reap = |inflight: &mut Vec<WriteHandle>| -> Result<(), ZoneStoreError> {
        let mut i = 0;
        let mut result = Ok(());
        while i < inflight.len() {
            if inflight[i].is_done() {
                let h = inflight.swap_remove(i);
                if let Err(e) = h.into_result() {
                    result = Err(ZoneStoreError::Block(e));
                }
            } else {
                i += 1;
            }
        }
        result
    };
    for s in 0..num_segments {
        while inflight.len() >= MAX_INFLIGHT_WIPES {
            let head_done = inflight.first().is_some_and(|h| h.is_done());
            if dev.poll_writes() > 0 || head_done {
                reap(&mut inflight)?;
            }
        }
        vals_store::attribution::write_bytes(6, zero.len());
        let h = dev.submit_write(cfg.reserved_blocks + s * cfg.seg_blocks, &zero);
        inflight.push(h);
    }
    while !inflight.is_empty() {
        let head_done = inflight.first().is_some_and(|h| h.is_done());
        if dev.poll_writes() > 0 || head_done {
            reap(&mut inflight)?;
        }
    }
    dev.flush()?;
    Ok(())
}

pub struct SuperblockStore<D: BlockDevice> {
    dev: D,
    block_size: usize,
    slot_blocks: u64,
}

impl<D: BlockDevice> SuperblockStore<D> {
    pub fn new(dev: D, slot_blocks: u64) -> Self {
        let block_size = dev.block_size();
        SuperblockStore {
            dev,
            block_size,
            slot_blocks,
        }
    }

    fn slot_bytes(&self) -> usize {
        self.slot_blocks as usize * self.block_size
    }

    pub fn persist(
        &self,
        generation: u64,
        regions: &[ZoneRegion],
        watermark: Option<u64>,
        topology: Option<ZoneTopology>,
        incarnation: Option<u128>,
    ) -> Result<(), ZoneStoreError> {
        let rec = encode_superblock(generation, regions, watermark, topology, incarnation);
        let slot_bytes = self.slot_bytes();
        if rec.len() > slot_bytes {
            return Err(ZoneStoreError::Corrupt("superblock record exceeds slot"));
        }
        let mut buf = vec![0u8; slot_bytes];
        buf[..rec.len()].copy_from_slice(&rec);
        let lba = (generation % 2) * self.slot_blocks;
        vals_store::attribution::write_bytes(5, buf.len());
        self.dev.write_at(lba, &buf)?;
        self.dev.flush()?;
        Ok(())
    }

    pub fn reformat(
        &self,
        regions: &[ZoneRegion],
        watermark: Option<u64>,
        topology: Option<ZoneTopology>,
    ) -> Result<(), ZoneStoreError> {
        let zero = vec![0u8; self.slot_bytes()];
        vals_store::attribution::write_bytes(5, zero.len());
        self.dev.write_at(self.slot_blocks, &zero)?;
        self.dev.flush()?;
        self.persist(0, regions, watermark, topology, None)
    }

    pub fn load(&self) -> Result<Option<SuperRecord>, ZoneStoreError> {
        let slot_bytes = self.slot_bytes();
        let mut best: Option<SuperRecord> = None;
        for slot in 0..2u64 {
            let mut buf = vec![0u8; slot_bytes];
            self.dev.read_at(slot * self.slot_blocks, &mut buf)?;
            if let Some(record) = decode_superblock(&buf)? {
                if best.as_ref().is_none_or(|best| record.0 > best.0) {
                    best = Some(record);
                }
            }
        }
        Ok(best)
    }
}

type LoadedSuper = (RegionTable, u64, Option<u64>, Option<ZoneTopology>, Option<u128>);

#[allow(clippy::too_many_arguments)]
fn load_or_init_superblock<D: BlockDevice>(
    super_store: &SuperblockStore<D>,
    super_blocks: u64,
    region_blocks: u64,
    total_blocks: u64,
    alignment_blocks: u64,
    min_region_blocks: u64,
    overrides: &[(ZoneId, u64)],
    create: bool,
    expected: Option<ZoneTopology>,
) -> Result<LoadedSuper, ZoneStoreError> {
    if create {
        let table = RegionTable::new_variable(
            super_blocks,
            region_blocks,
            total_blocks,
            alignment_blocks,
            min_region_blocks,
            overrides,
        )?;
        super_store.reformat(&[], None, expected)?;
        return Ok((table, 0, None, expected, None));
    }
    match super_store.load()? {
        Some((generation, regions, watermark, topology, incarnation)) => {
            if let Some(expected) = expected {
                verify_topology(topology, expected)?;
            }
            Ok((
                RegionTable::from_regions_variable(
                    super_blocks,
                    region_blocks,
                    total_blocks,
                    alignment_blocks,
                    min_region_blocks,
                    overrides,
                    regions,
                )?,
                generation,
                watermark,
                topology,
                incarnation,
            ))
        }
        None => Err(ZoneStoreError::Corrupt(
            "no zone superblock: drive holds a pre-zoned layout or is blank; run \
             `valsd provision` while the node is offline",
        )),
    }
}

pub fn verify_topology(
    topology: Option<ZoneTopology>, expected: ZoneTopology,
) -> Result<(), ZoneStoreError> {
    let topology = topology.ok_or(ZoneStoreError::Corrupt(
        "legacy zone layout lacks a topology descriptor; run `valsd provision` while the node is offline",
    ))?;
    topology.verify(expected)
}

struct SuperState<SD: BlockDevice> {
    store: SuperblockStore<SD>,
    table: RegionTable,
    generation: u64,
    watermark: Option<u64>,
    topology: Option<ZoneTopology>,
    incarnation: Option<u128>,
}

impl<SD: BlockDevice> SuperState<SD> {
    fn persist_watermark(&mut self, watermark: u64) -> Result<(), ZoneStoreError> {
        self.generation += 1;
        let gen = self.generation;
        let regions = self.table.regions();
        self.store.persist(gen, &regions, Some(watermark), self.topology, self.incarnation)?;
        self.watermark = Some(watermark);
        Ok(())
    }

    fn persist_table(&mut self) -> Result<(), ZoneStoreError> {
        self.generation += 1;
        let gen = self.generation;
        let wm = self.watermark;
        let regions = self.table.regions();
        self.store.persist(gen, &regions, wm, self.topology, self.incarnation)
    }

    fn persist_incarnation(&mut self, incarnation: Option<u128>) -> Result<(), ZoneStoreError> {
        let regions = self.table.regions();
        let slots = if incarnation.is_some() { 1 } else { 2 };
        for _ in 0..slots {
            self.generation += 1;
            let gen = self.generation;
            self.store.persist(gen, &regions, self.watermark, self.topology, incarnation)?;
        }
        self.incarnation = incarnation;
        Ok(())
    }
}

pub struct SuperblockLivenessLog<SD: BlockDevice> {
    state: Arc<Mutex<SuperState<SD>>>,
}

impl<SD: BlockDevice> Clone for SuperblockLivenessLog<SD> {
    fn clone(&self) -> Self {
        SuperblockLivenessLog {
            state: self.state.clone(),
        }
    }
}

impl<SD: BlockDevice> LivenessLog for SuperblockLivenessLog<SD> {
    fn load(&self) -> Option<u64> {
        self.state.lock().unwrap().watermark
    }

    fn store(&self, watermark_ms: u64) -> std::io::Result<()> {
        self.state
            .lock()
            .unwrap()
            .persist_watermark(watermark_ms)
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    fn record_wipe(&self, stats: &WipeStats) -> std::io::Result<()> {
        log::warn!(
            "liveness wipe (on-device log, best-effort marker): at_ms={} local_wm_ms={} \
             max_peer_wm_ms={} gc_grace_ms={} zones={} keys={}",
            stats.wall_ms,
            stats.local_watermark_ms,
            stats.max_peer_watermark_ms,
            stats.gc_grace_ms,
            stats.zones,
            stats.keys
        );
        Ok(())
    }
}

pub struct SubDev<D: BlockDevice> {
    parent: Arc<D>,
    base: u64,
    len: u64,
}

impl<D: BlockDevice> SubDev<D> {
    pub fn new(parent: Arc<D>, base: u64, len: u64) -> Self {
        SubDev { parent, base, len }
    }

    fn check(&self, lba: u64, len_bytes: usize) -> BlockResult<()> {
        let bs = self.parent.block_size();
        if !len_bytes.is_multiple_of(bs) {
            return Err(BlockError::Misaligned {
                len: len_bytes,
                block_size: bs,
            });
        }
        let blocks = (len_bytes / bs) as u64;
        if lba.checked_add(blocks).is_none_or(|end| end > self.len) {
            return Err(BlockError::OutOfRange {
                lba,
                blocks,
                num_blocks: self.len,
            });
        }
        Ok(())
    }
}

thread_local! {
    static MAINT_PARENT_SIBLINGS: RefCell<Vec<(usize, Box<dyn std::any::Any>)>> =
        const { RefCell::new(Vec::new()) };
}

impl<D: BlockDevice> BlockDevice for SubDev<D> {
    fn block_size(&self) -> usize {
        self.parent.block_size()
    }
    fn num_blocks(&self) -> u64 {
        self.len
    }
    fn read_at(&self, lba: u64, buf: &mut [u8]) -> BlockResult<()> {
        self.check(lba, buf.len())?;
        self.parent.read_at(self.base + lba, buf)
    }
    fn write_at(&self, lba: u64, buf: &[u8]) -> BlockResult<()> {
        self.check(lba, buf.len())?;
        self.parent.write_at(self.base + lba, buf)
    }
    fn flush(&self) -> BlockResult<()> {
        self.parent.flush()
    }
    fn deallocate(&self, ranges: &[(u64, u64)]) -> BlockResult<bool> {
        vals_spdk::check_dealloc_ranges(self.len, ranges)?;
        let shifted: Vec<_> = ranges.iter().map(|&(lba, blocks)| (self.base + lba, blocks)).collect();
        self.parent.deallocate(&shifted)
    }
    fn read_batch(&self, ops: &mut [ReadOp<'_>]) -> BlockResult<()> {
        for op in ops.iter_mut() {
            self.check(op.lba, op.buf.len())?;
            op.lba += self.base;
        }
        let r = self.parent.read_batch(ops);
        for op in ops.iter_mut() {
            op.lba -= self.base;
        }
        r
    }
    fn submit_write(&self, lba: u64, buf: &[u8]) -> WriteHandle {
        if let Err(e) = self.check(lba, buf.len()) {
            return WriteHandle::ready(Err(e));
        }
        self.parent.submit_write(self.base + lba, buf)
    }
    fn submit_flush(&self) -> Option<WriteHandle> {
        self.parent.submit_flush()
    }
    fn flush_completes_off_thread(&self) -> bool {
        self.parent.flush_completes_off_thread()
    }
    fn flush_domain(&self) -> Option<usize> {
        self.parent.flush_domain()
    }
    fn poll_writes(&self) -> i32 {
        self.parent.poll_writes()
    }
    fn poll_device(&self) -> i32 {
        self.parent.poll_device()
    }
    fn bind_io_owner(&self) {
        self.parent.bind_io_owner()
    }
    fn maintenance_sibling_cache_key(&self) -> Option<MaintenanceCacheKey> {
        Some(MaintenanceCacheKey::new(
            Arc::as_ptr(&self.parent) as *const () as usize,
            self.base,
            self.len,
        ))
    }
    fn take_scratch(&self, len: usize) -> Scratch {
        self.parent.take_scratch(len)
    }
    fn release_scratch(&self, buf: Scratch) {
        self.parent.release_scratch(buf)
    }
    fn sibling(&self) -> BlockResult<Self> {
        use std::any::Any;
        let key = Arc::as_ptr(&self.parent) as *const () as usize;

        let hit = MAINT_PARENT_SIBLINGS.with(|cell| {
            let mut cache = cell.borrow_mut();
            cache.retain(|(_, payload)| {
                payload
                    .downcast_ref::<(std::sync::Weak<D>, Arc<D>)>()
                    .map(|(w, _)| w.upgrade().is_some())
                    .unwrap_or(true)
            });
            cache.iter().find_map(|(k, payload)| {
                if *k != key {
                    return None;
                }
                payload
                    .downcast_ref::<(std::sync::Weak<D>, Arc<D>)>()
                    .map(|(_, shared)| shared.clone())
            })
        });

        let shared = match hit {
            Some(s) => s,
            None => {
                let sib = Arc::new(self.parent.as_ref().sibling()?);
                MAINT_PARENT_SIBLINGS.with(|cell| {
                    let payload: Box<dyn Any> =
                        Box::new((Arc::downgrade(&self.parent), sib.clone()));
                    cell.borrow_mut().push((key, payload));
                });
                sib
            }
        };
        Ok(SubDev {
            parent: shared,
            base: self.base,
            len: self.len,
        })
    }

    fn maintenance_sibling(&self) -> BlockResult<Self> {
        Ok(SubDev {
            parent: Arc::new(self.parent.as_ref().maintenance_sibling()?),
            base: self.base,
            len: self.len,
        })
    }
}

impl<D: BlockDevice + AsyncRead> AsyncRead for SubDev<D> {
    async fn read_async(&self, lba: u64, buf: &mut [u8]) -> BlockResult<()> {
        self.check(lba, buf.len())?;
        self.parent.read_async(self.base + lba, buf).await
    }
}

#[derive(Debug, Clone)]
pub struct RegionZoneConfig {
    pub super_blocks: u64,
    pub zone_region_blocks: u64,
    pub zone_region_overrides: Vec<(ZoneId, u64)>,
    pub seg_blocks: u64,
    pub reserved_blocks: u64,
}

impl RegionZoneConfig {
    fn geometry_fingerprint(&self, block_size: usize) -> u64 {
        let mut bytes = Vec::new();
        for n in [block_size as u64, self.super_blocks, self.seg_blocks, self.reserved_blocks] {
            bytes.extend_from_slice(&n.to_le_bytes());
        }
        crate::ring::hash64(&bytes)
    }
}

pub fn open_partition_zone_stores<DS: crate::drive::DriveSet>(
    ds: &DS, layout: &crate::drive::Layout, cfg: &RegionZoneConfig, node_id: u32, create: bool,
) -> Result<Vec<RegionZoneStore<DS::Dev>>, crate::drive::LayoutError> {
    let zs_err = |e: ZoneStoreError| crate::drive::LayoutError::Io(std::io::Error::other(e.to_string()));
    for p in layout.partitions() {
        validate_region_zone_layout(cfg, p.len).map_err(zs_err)?;
    }
    let parents = layout.partitions().iter()
        .map(|p| ds.open_region(p.drive, p.base, p.len, create).map(Arc::new))
        .collect::<Result<Vec<_>, _>>()?;
    crate::drive::verify_layout(ds, layout, create)?;
    let layout_id = fresh_layout_id();
    let mut providers = Vec::with_capacity(layout.num_partitions());
    let mut loaded_id = None;
    for (p, parent) in layout.partitions().iter().zip(parents) {
        let expected = ZoneTopology::partition(
            layout_id, node_id, layout.num_partitions(), p, cfg, parent.block_size(),
        );
        let provider = RegionZoneStore::open_checked(parent, cfg, node_id, create, expected)
            .map_err(zs_err)?;
        let id = provider.layout_id().unwrap();
        if loaded_id.is_some_and(|previous| previous != id) {
            return Err(zs_err(ZoneStoreError::Corrupt(
                "partition layout identities differ (incomplete provisioning); run `valsd provision` while the node is offline",
            )));
        }
        loaded_id = Some(id);
        providers.push(provider);
    }
    Ok(providers)
}

pub struct RegionZoneStore<D: BlockDevice> {
    node_id: u32,
    parent: Arc<D>,
    seg_blocks: u64,
    reserved_blocks: u64,
    state: Arc<Mutex<SuperState<SubDev<D>>>>,
}

pub fn validate_region_zone_layout(
    cfg: &RegionZoneConfig,
    total_blocks: u64,
) -> Result<(), ZoneStoreError> {
    if cfg.super_blocks < 2 || !cfg.super_blocks.is_multiple_of(2) {
        return Err(ZoneStoreError::Corrupt(
            "zone superblock extent must be even and at least two blocks",
        ));
    }
    let min_region_blocks = cfg.reserved_blocks.checked_add(cfg.seg_blocks).ok_or(
        ZoneStoreError::Corrupt("configured minimum zone region overflows"),
    )?;
    RegionTable::new_variable(
        cfg.super_blocks,
        cfg.zone_region_blocks,
        total_blocks,
        cfg.seg_blocks,
        min_region_blocks,
        &cfg.zone_region_overrides,
    )?;
    for region_blocks in std::iter::once(cfg.zone_region_blocks)
        .chain(cfg.zone_region_overrides.iter().map(|(_, blocks)| *blocks))
    {
        let segments = region_blocks.saturating_sub(cfg.reserved_blocks) / cfg.seg_blocks;
        if segments > u32::MAX as u64 + 1 {
            return Err(ZoneStoreError::Corrupt(
                "configured zone has too many segments for the engine format",
            ));
        }
    }
    Ok(())
}

impl<D: BlockDevice> RegionZoneStore<D> {
    #[cfg(test)]
    pub fn provision<C: Borrow<RegionZoneConfig>>(
        parent: Arc<D>,
        cfg: C,
        node_id: u32,
    ) -> Result<Self, ZoneStoreError> {
        let store = Self::open(parent, cfg, node_id, true)?;
        store.open(crate::zone::DEFAULT_ZONE, Ordering::Unsorted)?;
        Ok(store)
    }

    pub fn open_existing(
        &self,
        zone_id: ZoneId,
    ) -> Result<VersionedStore<SubDev<D>>, ZoneStoreError> {
        let ordering = self.zone_ordering(zone_id).ok_or(ZoneStoreError::Corrupt(
            "required zone is absent; run `valsd provision` while the node is offline",
        ))?;
        self.open(zone_id, ordering)
    }

    #[cfg(test)]
    pub fn open<C: Borrow<RegionZoneConfig>>(
        parent: Arc<D>,
        cfg: C,
        node_id: u32,
        create: bool,
    ) -> Result<Self, ZoneStoreError> {
        Self::open_inner(parent, cfg.borrow(), node_id, create, None)
    }

    pub fn open_checked(
        parent: Arc<D>, cfg: &RegionZoneConfig, node_id: u32, create: bool, expected: ZoneTopology,
    ) -> Result<Self, ZoneStoreError> {
        Self::open_inner(parent, cfg, node_id, create, Some(expected))
    }

    pub fn layout_id(&self) -> Option<u64> {
        self.state.lock().unwrap().topology.map(|t| t.layout_id)
    }

    fn open_inner(
        parent: Arc<D>, cfg: &RegionZoneConfig, node_id: u32, create: bool,
        expected: Option<ZoneTopology>,
    ) -> Result<Self, ZoneStoreError> {
        let total_blocks = parent.num_blocks();
        validate_region_zone_layout(cfg, total_blocks)?;
        let super_dev = SubDev::new(parent.clone(), 0, cfg.super_blocks);
        let super_store = SuperblockStore::new(super_dev, cfg.super_blocks / 2);

        let min_region_blocks = cfg.reserved_blocks.checked_add(cfg.seg_blocks).ok_or(
            ZoneStoreError::Corrupt("configured minimum zone region overflows"),
        )?;
        let (table, generation, watermark, topology, incarnation) = load_or_init_superblock(
            &super_store,
            cfg.super_blocks,
            cfg.zone_region_blocks,
            total_blocks,
            cfg.seg_blocks,
            min_region_blocks,
            &cfg.zone_region_overrides,
            create,
            expected,
        )?;

        Ok(RegionZoneStore {
            node_id,
            parent,
            seg_blocks: cfg.seg_blocks,
            reserved_blocks: cfg.reserved_blocks,
            state: Arc::new(Mutex::new(SuperState {
                store: super_store,
                table,
                generation,
                watermark,
                topology,
                incarnation,
            })),
        })
    }

    fn engine_config(&self, ordering: Ordering) -> Config {
        Config {
            seg_blocks: self.seg_blocks,
            reserved_blocks: self.reserved_blocks,
            ordering: engine_ordering(ordering),
        }
    }

    pub fn open_engine_readonly(&self, zone_id: ZoneId) -> Result<Engine<SubDev<D>>, ZoneStoreError> {
        let state = self.state.lock().unwrap();
        let region = state.table.get(zone_id).ok_or(ZoneStoreError::Corrupt(
            "requested zone is absent from the region table",
        ))?;
        let dev = SubDev::new(self.parent.clone(), region.base, region.len);
        Ok(Engine::open(dev, self.engine_config(region.ordering))?)
    }

    pub fn liveness_log(&self) -> SuperblockLivenessLog<SubDev<D>> {
        SuperblockLivenessLog {
            state: self.state.clone(),
        }
    }
}

impl<D: BlockDevice> ZoneStore<SubDev<D>> for RegionZoneStore<D> {
    fn open(
        &self,
        zone_id: ZoneId,
        ordering: Ordering,
    ) -> Result<VersionedStore<SubDev<D>>, ZoneStoreError> {
        let mut state = self.state.lock().unwrap();
        if let Some(region) = state.table.get(zone_id) {
            let dev = SubDev::new(self.parent.clone(), region.base, region.len);
            return Ok(VersionedStore::open(
                dev,
                self.engine_config(region.ordering),
                self.node_id,
            )?);
        }
        let cfg = self.engine_config(ordering);
        let region = state.table.alloc(zone_id, ordering)?;
        let dev = SubDev::new(self.parent.clone(), region.base, region.len);
        wipe_region(&dev, &cfg)?;
        state.persist_table()?;
        let engine = Engine::create_prewiped(dev, cfg);
        Ok(VersionedStore::new(engine, self.node_id)?)
    }

    fn drop_zone(&self, zone_id: ZoneId) -> Result<(), ZoneStoreError> {
        let mut state = self.state.lock().unwrap();
        if state.table.free_zone(zone_id).is_some() {
            state.persist_table()?;
        }
        Ok(())
    }

    fn existing(&self) -> Vec<ZoneId> {
        self.state.lock().unwrap().table.zone_ids()
    }

    fn zone_ordering(&self, zone_id: ZoneId) -> Option<Ordering> {
        self.state
            .lock()
            .unwrap()
            .table
            .get(zone_id)
            .map(|r| r.ordering)
    }

    fn persisted_incarnation(&self) -> Option<u128> {
        self.state.lock().unwrap().incarnation
    }

    fn persist_incarnation(&self, incarnation: Option<u128>) -> Result<(), ZoneStoreError> {
        self.state.lock().unwrap().persist_incarnation(incarnation)
    }
}

#[cfg(feature = "spdk")]
#[derive(Debug, Clone, Copy)]
pub struct SpdkZoneConfig {
    pub super_blocks: u64,
    pub region_blocks: u64,
    pub seg_blocks: u64,
    pub reserved_blocks: u64,
}

#[cfg(feature = "spdk")]
pub struct SpdkZoneStore {
    node_id: u32,
    controller: std::sync::Arc<vals_spdk::spdk::SpdkController>,
    seg_blocks: u64,
    reserved_blocks: u64,
    state: Arc<Mutex<SuperState<vals_spdk::spdk::SpdkBlockDevice>>>,
}

#[cfg(feature = "spdk")]
impl SpdkZoneStore {
    pub fn provision(
        node_id: u32,
        controller: std::sync::Arc<vals_spdk::spdk::SpdkController>,
        cfg: SpdkZoneConfig,
    ) -> Result<Self, ZoneStoreError> {
        let store = Self::open(node_id, controller, cfg, true)?;
        store.open(crate::zone::DEFAULT_ZONE, Ordering::Unsorted)?;
        Ok(store)
    }

    pub fn open_existing(
        &self,
        zone_id: ZoneId,
    ) -> Result<VersionedStore<vals_spdk::spdk::SpdkBlockDevice>, ZoneStoreError> {
        let ordering = self.zone_ordering(zone_id).ok_or(ZoneStoreError::Corrupt(
            "required zone is absent; run `valsd provision` while the node is offline",
        ))?;
        self.open(zone_id, ordering)
    }

    pub fn open(
        node_id: u32,
        controller: std::sync::Arc<vals_spdk::spdk::SpdkController>,
        cfg: SpdkZoneConfig,
        create: bool,
    ) -> Result<Self, ZoneStoreError> {
        let total_blocks = controller.num_blocks();
        let region_cfg = RegionZoneConfig {
            super_blocks: cfg.super_blocks,
            zone_region_blocks: cfg.region_blocks,
            zone_region_overrides: Vec::new(),
            seg_blocks: cfg.seg_blocks,
            reserved_blocks: cfg.reserved_blocks,
        };
        validate_region_zone_layout(&region_cfg, total_blocks)?;
        let super_dev = controller.open_shard(0, cfg.super_blocks)?;
        let expected = ZoneTopology::single(node_id, total_blocks, &region_cfg, super_dev.block_size());
        let super_store = SuperblockStore::new(super_dev, cfg.super_blocks / 2);

        let min_region_blocks = cfg.reserved_blocks.checked_add(cfg.seg_blocks).ok_or(
            ZoneStoreError::Corrupt("configured minimum zone region overflows"),
        )?;
        let (table, generation, watermark, topology, incarnation) = load_or_init_superblock(
            &super_store,
            cfg.super_blocks,
            cfg.region_blocks,
            total_blocks,
            cfg.seg_blocks,
            min_region_blocks,
            &[],
            create,
            Some(expected),
        )?;

        Ok(SpdkZoneStore {
            node_id,
            controller,
            seg_blocks: cfg.seg_blocks,
            reserved_blocks: cfg.reserved_blocks,
            state: Arc::new(Mutex::new(SuperState {
                store: super_store,
                table,
                generation,
                watermark,
                topology,
                incarnation,
            })),
        })
    }

    fn engine_config(&self, ordering: Ordering) -> Config {
        Config {
            seg_blocks: self.seg_blocks,
            reserved_blocks: self.reserved_blocks,
            ordering: engine_ordering(ordering),
        }
    }

    pub fn liveness_log(&self) -> SuperblockLivenessLog<vals_spdk::spdk::SpdkBlockDevice> {
        SuperblockLivenessLog {
            state: self.state.clone(),
        }
    }
}

#[cfg(feature = "spdk")]
impl ZoneStore<vals_spdk::spdk::SpdkBlockDevice> for SpdkZoneStore {
    fn open(
        &self,
        zone_id: ZoneId,
        ordering: Ordering,
    ) -> Result<VersionedStore<vals_spdk::spdk::SpdkBlockDevice>, ZoneStoreError> {
        let mut state = self.state.lock().unwrap();
        if let Some(region) = state.table.get(zone_id) {
            let dev = self.controller.open_shard(region.base, region.len)?;
            return Ok(VersionedStore::open(
                dev,
                self.engine_config(region.ordering),
                self.node_id,
            )?);
        }
        let cfg = self.engine_config(ordering);
        let region = state.table.alloc(zone_id, ordering)?;
        let dev = self.controller.open_shard(region.base, region.len)?;
        wipe_region(&dev, &cfg)?;
        state.persist_table()?;
        let engine = Engine::create_prewiped(dev, cfg);
        Ok(VersionedStore::new(engine, self.node_id)?)
    }

    fn drop_zone(&self, zone_id: ZoneId) -> Result<(), ZoneStoreError> {
        let mut state = self.state.lock().unwrap();
        if state.table.free_zone(zone_id).is_some() {
            state.persist_table()?;
        }
        Ok(())
    }

    fn existing(&self) -> Vec<ZoneId> {
        self.state.lock().unwrap().table.zone_ids()
    }

    fn zone_ordering(&self, zone_id: ZoneId) -> Option<Ordering> {
        self.state
            .lock()
            .unwrap()
            .table
            .get(zone_id)
            .map(|r| r.ordering)
    }

    fn persisted_incarnation(&self) -> Option<u128> {
        self.state.lock().unwrap().incarnation
    }

    fn persist_incarnation(&self, incarnation: Option<u128>) -> Result<(), ZoneStoreError> {
        self.state.lock().unwrap().persist_incarnation(incarnation)
    }
}

#[cfg(test)]
mod topology_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::Version;

    fn small_geom() -> ZoneGeom {
        ZoneGeom {
            block_size: 256,
            seg_blocks: 4,
            reserved_blocks: 0,
            region_blocks: 512,
        }
    }

    #[test]
    fn superblock_codec_roundtrips() {
        let regions = vec![
            ZoneRegion {
                zone_id: 0,
                base: 32,
                len: 512,
                ordering: Ordering::Unsorted,
            },
            ZoneRegion {
                zone_id: 7,
                base: 544,
                len: 512,
                ordering: Ordering::Sorted,
            },
        ];
        let enc = encode_superblock(42, &regions, None, None, None);
        let (gen, dec, wm, _, _) = decode_superblock(&enc).unwrap().unwrap();
        assert_eq!(gen, 42);
        assert_eq!(dec, regions);
        assert_eq!(wm, None);

        let enc_wm = encode_superblock(7, &regions, Some(123_456), None, None);
        let (gen_wm, dec_wm, wm2, _, _) = decode_superblock(&enc_wm).unwrap().unwrap();
        assert_eq!(gen_wm, 7);
        assert_eq!(dec_wm, regions);
        assert_eq!(wm2, Some(123_456));

        let (gen0, empty, wm0, _, _) = decode_superblock(&encode_superblock(0, &[], None, None, None))
            .unwrap()
            .unwrap();
        assert_eq!(gen0, 0);
        assert!(empty.is_empty());
        assert_eq!(wm0, None);
    }

    #[test]
    fn superblock_decodes_v1_as_watermark_absent() {
        let regions = vec![
            ZoneRegion {
                zone_id: 3,
                base: 40,
                len: 512,
                ordering: Ordering::Sorted,
            },
            ZoneRegion {
                zone_id: 9,
                base: 552,
                len: 512,
                ordering: Ordering::Unsorted,
            },
        ];
        let mut buf = Vec::new();
        buf.extend_from_slice(&SUPER_MAGIC.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.extend_from_slice(&11u64.to_le_bytes());
        buf.extend_from_slice(&(regions.len() as u32).to_le_bytes());
        for r in &regions {
            buf.extend_from_slice(&r.zone_id.to_le_bytes());
            buf.extend_from_slice(&r.base.to_le_bytes());
            buf.extend_from_slice(&r.len.to_le_bytes());
            buf.push(ordering_to_u8(r.ordering));
        }
        let crc = crc32(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        let (gen, dec, wm, topology, _) = decode_superblock(&buf).unwrap().unwrap();
        assert_eq!(topology, None);
        assert_eq!(gen, 11);
        assert_eq!(dec, regions);
        assert_eq!(
            wm, None,
            "a v1 record reads as watermark-absent (conservative)"
        );
    }

    #[test]
    fn superblock_codec_rejects_corruption() {
        let regions = vec![ZoneRegion {
            zone_id: 1,
            base: 32,
            len: 512,
            ordering: Ordering::Unsorted,
        }];
        let enc = encode_superblock(5, &regions, None, None, None);

        let mut bad = enc.clone();
        bad[0] ^= 0xFF;
        assert!(decode_superblock(&bad).unwrap().is_none());

        let mut bad = enc.clone();
        bad[SUPER_HEADER_BYTES] ^= 0x01;
        assert!(decode_superblock(&bad).unwrap().is_none());

        assert!(decode_superblock(&enc[..enc.len() - 2]).unwrap().is_none());

        assert!(decode_superblock(&[0u8; 64]).unwrap().is_none());

        let mut future = enc.clone();
        future[4..6].copy_from_slice(&(SUPER_VERSION + 1).to_le_bytes());
        let body_end = future.len() - 4;
        let crc = crc32(&future[..body_end]);
        future[body_end..].copy_from_slice(&crc.to_le_bytes());
        assert!(
            matches!(decode_superblock(&future), Err(ZoneStoreError::Corrupt(_))),
            "an intact superblock with an unknown version must be rejected loud"
        );
    }

    #[test]
    fn region_table_bumps_then_reuses_freed() {
        let mut t = RegionTable::new(32, 512, 32 + 3 * 512);
        assert_eq!(t.capacity(), 3);
        let a = t.alloc(1, Ordering::Unsorted).unwrap();
        let b = t.alloc(2, Ordering::Unsorted).unwrap();
        let c = t.alloc(3, Ordering::Sorted).unwrap();
        assert_eq!(a.base, 32);
        assert_eq!(b.base, 32 + 512);
        assert_eq!(c.base, 32 + 2 * 512);

        match t.alloc(4, Ordering::Unsorted) {
            Err(ZoneStoreError::Exhausted { capacity_zones }) => assert_eq!(capacity_zones, 3),
            other => panic!("expected Exhausted, got {other:?}"),
        }

        let freed = t.free_zone(2).unwrap();
        assert_eq!(freed.base, 32 + 512);
        let d = t.alloc(4, Ordering::Unsorted).unwrap();
        assert_eq!(d.base, 32 + 512, "freed region is reused");
        assert_eq!(t.zone_ids(), vec![1, 3, 4]);
    }

    #[test]
    fn region_table_variable_extents_use_overrides_and_fallback() {
        let mut t = RegionTable::new_variable(16, 64, 16 + 8 * 64, 16, 16, &[(0, 16), (2, 128)])
            .unwrap();
        let default = t.alloc(0, Ordering::Unsorted).unwrap();
        let fallback = t.alloc(1, Ordering::Sorted).unwrap();
        let large = t.alloc(2, Ordering::Sorted).unwrap();
        assert_eq!((default.base, default.len), (16, 16));
        assert_eq!((fallback.base, fallback.len), (32, 64));
        assert_eq!((large.base, large.len), (96, 128));
    }

    #[test]
    fn region_table_variable_extents_coalesce_freed_gaps_first_fit() {
        let mut t = RegionTable::new_variable(
            16,
            64,
            16 + 8 * 64,
            16,
            16,
            &[(1, 32), (2, 64), (3, 32), (4, 96)],
        )
        .unwrap();
        let a = t.alloc(1, Ordering::Unsorted).unwrap();
        let _b = t.alloc(2, Ordering::Unsorted).unwrap();
        let _c = t.alloc(3, Ordering::Unsorted).unwrap();
        t.free_zone(2).unwrap();
        t.free_zone(1).unwrap();
        let combined = t.alloc(4, Ordering::Unsorted).unwrap();
        assert_eq!(combined.base, a.base, "adjacent freed extents must coalesce");

        let mut t = RegionTable::new_variable(
            16,
            64,
            16 + 8 * 64,
            16,
            16,
            &[(1, 32), (2, 32), (3, 64), (4, 32), (5, 96)],
        )
        .unwrap();
        let left = t.alloc(1, Ordering::Unsorted).unwrap();
        let _separator = t.alloc(2, Ordering::Unsorted).unwrap();
        let right = t.alloc(3, Ordering::Unsorted).unwrap();
        let _tail = t.alloc(4, Ordering::Unsorted).unwrap();
        t.free_zone(1).unwrap();
        t.free_zone(4).unwrap();
        t.free_zone(3).unwrap();
        let combined = t.alloc(5, Ordering::Unsorted).unwrap();
        assert_ne!(combined.base, left.base);
        assert_eq!(combined.base, right.base, "a right-only neighbor must coalesce");
    }

    #[test]
    fn region_table_variable_recovery_rejects_bad_extents() {
        let region = |zone_id, base, len| ZoneRegion {
            zone_id,
            base,
            len,
            ordering: Ordering::Unsorted,
        };
        let recover = |regions| {
            RegionTable::from_regions_variable(16, 64, 512, 16, 16, &[], regions)
        };
        assert!(recover(vec![region(1, 16, 8)]).is_err());
        assert!(recover(vec![region(1, 24, 16)]).is_err());
        assert!(recover(vec![region(1, 16, 64), region(2, 64, 64)]).is_err());
        assert!(recover(vec![region(1, 496, 32)]).is_err());
        assert!(recover(vec![region(1, u64::MAX - 15, 32)]).is_err());
        assert!(recover(vec![region(1, 16, 16), region(1, 32, 16)]).is_err());
        assert!(RegionTable::new_variable(16, 64, 512, 16, 16, &[(7, 512)]).is_err());
        assert!(RegionTable::new_variable(16, 512, 512, 16, 16, &[]).is_err());
    }

    #[test]
    fn region_table_variable_exhaustion_is_isolated() {
        let mut t = RegionTable::new_variable(16, 64, 16 + 128, 16, 16, &[(1, 96), (2, 48)])
            .unwrap();
        let first = t.alloc(1, Ordering::Unsorted).unwrap();
        assert!(matches!(
            t.alloc(2, Ordering::Unsorted),
            Err(ZoneStoreError::Exhausted { .. })
        ));
        assert_eq!(t.get(1), Some(first));
        assert_eq!(t.zone_ids(), vec![1]);
    }

    #[test]
    fn region_table_alloc_is_idempotent_per_zone() {
        let mut t = RegionTable::new(32, 512, 32 + 4 * 512);
        let a = t.alloc(9, Ordering::Unsorted).unwrap();
        let again = t.alloc(9, Ordering::Sorted).unwrap();
        assert_eq!(a, again, "re-alloc returns the existing region");
        assert_eq!(t.regions().len(), 1);
    }

    #[test]
    fn region_table_recovers_holes() {
        let recovered = vec![
            ZoneRegion {
                zone_id: 1,
                base: 32,
                len: 512,
                ordering: Ordering::Unsorted,
            },
            ZoneRegion {
                zone_id: 3,
                base: 32 + 2 * 512,
                len: 512,
                ordering: Ordering::Sorted,
            },
        ];
        let mut t = RegionTable::from_regions(32, 512, 32 + 8 * 512, recovered).unwrap();
        assert_eq!(t.zone_ids(), vec![1, 3]);
        let n = t.alloc(5, Ordering::Unsorted).unwrap();
        assert_eq!(n.base, 32 + 512, "recovered hole reused");
        let m = t.alloc(6, Ordering::Unsorted).unwrap();
        assert_eq!(m.base, 32 + 3 * 512);
    }

    #[test]
    fn region_table_rejects_geometry_mismatch() {
        let region = |base: u64, len: u64| ZoneRegion {
            zone_id: 1,
            base,
            len,
            ordering: Ordering::Unsorted,
        };
        assert!(matches!(
            RegionTable::from_regions(32, 256, 32 + 8 * 256, vec![region(32, 512)]),
            Err(ZoneStoreError::Corrupt(_))
        ));
        assert!(matches!(
            RegionTable::from_regions(32, 512, 32 + 8 * 512, vec![region(40, 512)]),
            Err(ZoneStoreError::Corrupt(_))
        ));
        assert!(matches!(
            RegionTable::from_regions(32, 512, 32 + 512, vec![region(32 + 512, 512)]),
            Err(ZoneStoreError::Corrupt(_))
        ));
        let dup = vec![
            region(32, 512),
            ZoneRegion {
                zone_id: 2,
                base: 32,
                len: 512,
                ordering: Ordering::Sorted,
            },
        ];
        assert!(matches!(
            RegionTable::from_regions(32, 512, 32 + 8 * 512, dup),
            Err(ZoneStoreError::Corrupt(_))
        ));
        assert!(RegionTable::from_regions(32, 512, 32 + 8 * 512, vec![region(32, 512)]).is_ok());
    }

    #[test]
    fn superblock_store_persists_and_recovers() {
        let dev = MemBlockDevice::new(256, 8);
        let store = SuperblockStore::new(dev.share(), 4);
        let regions = vec![
            ZoneRegion {
                zone_id: 1,
                base: 8,
                len: 512,
                ordering: Ordering::Unsorted,
            },
            ZoneRegion {
                zone_id: 2,
                base: 520,
                len: 512,
                ordering: Ordering::Sorted,
            },
        ];
        store.persist(1, &regions, None, None, None).unwrap();

        let reopened = SuperblockStore::new(dev.share(), 4);
        let (gen, recovered, wm, _, _) = reopened.load().unwrap().unwrap();
        assert_eq!(gen, 1);
        assert_eq!(recovered, regions);
        assert_eq!(wm, None);
    }

    #[test]
    fn superblock_store_persists_watermark() {
        let dev = MemBlockDevice::new(256, 8);
        let store = SuperblockStore::new(dev.share(), 4);
        let regions = vec![ZoneRegion {
            zone_id: 1,
            base: 8,
            len: 512,
            ordering: Ordering::Unsorted,
        }];
        store.persist(3, &regions, Some(777_000), None, None).unwrap();
        let (gen, recovered, wm, _, _) = SuperblockStore::new(dev.share(), 4)
            .load()
            .unwrap()
            .unwrap();
        assert_eq!(gen, 3);
        assert_eq!(recovered, regions);
        assert_eq!(wm, Some(777_000));
    }

    #[test]
    fn superblock_store_picks_higher_generation() {
        let dev = MemBlockDevice::new(256, 8);
        let store = SuperblockStore::new(dev.share(), 4);
        let g1 = vec![ZoneRegion {
            zone_id: 1,
            base: 8,
            len: 512,
            ordering: Ordering::Unsorted,
        }];
        let g2 = vec![
            ZoneRegion {
                zone_id: 1,
                base: 8,
                len: 512,
                ordering: Ordering::Unsorted,
            },
            ZoneRegion {
                zone_id: 2,
                base: 520,
                len: 512,
                ordering: Ordering::Unsorted,
            },
        ];
        store.persist(1, &g1, None, None, None).unwrap();
        store.persist(2, &g2, None, None, None).unwrap();
        let (gen, regions, _wm, _, _) = store.load().unwrap().unwrap();
        assert_eq!(gen, 2);
        assert_eq!(regions, g2);
    }

    #[test]
    fn superblock_store_fresh_device_is_empty() {
        let dev = MemBlockDevice::new(256, 8);
        let store = SuperblockStore::new(dev, 4);
        assert!(store.load().unwrap().is_none());
    }

    struct MemShard {
        inner: MemBlockDevice,
        base: u64,
        len: u64,
    }

    impl BlockDevice for MemShard {
        fn block_size(&self) -> usize {
            self.inner.block_size()
        }
        fn num_blocks(&self) -> u64 {
            self.len
        }
        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.inner.read_at(self.base + lba, buf)
        }
        fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
            self.inner.write_at(self.base + lba, buf)
        }
        fn flush(&self) -> vals_spdk::Result<()> {
            self.inner.flush()
        }
    }

    #[test]
    fn reused_region_must_not_resurrect_dropped_zone_data() {
        let cfg = Config {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: engine_ordering(Ordering::Unsorted),
        };
        let (super_blocks, region_blocks) = (8u64, 64u64);
        let total = super_blocks + 2 * region_blocks;
        let ns = MemBlockDevice::new(256, total);
        let shard = |r: &ZoneRegion| MemShard {
            inner: ns.share(),
            base: r.base,
            len: r.len,
        };
        let sb = SuperblockStore::new(
            MemShard {
                inner: ns.share(),
                base: 0,
                len: super_blocks,
            },
            super_blocks / 2,
        );
        let mut table = RegionTable::new(super_blocks, region_blocks, total);

        let rx = table.alloc(1, Ordering::Unsorted).unwrap();
        sb.persist(1, &table.regions(), None, None, None).unwrap();
        let mut ex = Engine::create_prewiped(shard(&rx), cfg);
        ex.put(b"x-key", b"x-secret", 0).unwrap();
        ex.sync().unwrap();
        drop(ex);

        table.free_zone(1).unwrap();
        sb.persist(2, &table.regions(), None, None, None).unwrap();

        let ry = table.alloc(2, Ordering::Unsorted).unwrap();
        assert_eq!(ry.base, rx.base, "the freed region is reused");
        let leaked = Engine::open(shard(&ry), cfg).unwrap();
        assert_eq!(
            leaked.get(b"x-key").unwrap().as_deref(),
            Some(&b"x-secret"[..]),
            "sanity: X's segments are still CRC-valid in the reused region"
        );
        drop(leaked);

        wipe_region(&shard(&ry), &cfg).unwrap();
        sb.persist(3, &table.regions(), None, None, None).unwrap();
        let _ey = Engine::create_prewiped(shard(&ry), cfg);

        let (gen, regions, _wm, _, _) = sb.load().unwrap().unwrap();
        assert_eq!(gen, 3);
        let table2 =
            RegionTable::from_regions(super_blocks, region_blocks, total, regions).unwrap();
        let r = table2.get(2).unwrap();
        assert_eq!(r.base, rx.base);
        let ey = Engine::open(shard(&r), cfg).unwrap();
        assert!(
            ey.is_empty(),
            "zone Y must reopen empty, not with the dropped zone's keys"
        );
        assert_eq!(
            ey.get(b"x-key").unwrap(),
            None,
            "the dropped zone's data must not resurrect under the new zone"
        );
    }

    #[test]
    fn region_zone_store_persists_watermark_and_regions_across_reopen() {
        use crate::liveness::LivenessLog;

        let cfg = RegionZoneConfig {
            super_blocks: 8,
            zone_region_blocks: 64,
            zone_region_overrides: Vec::new(),
            seg_blocks: 4,
            reserved_blocks: 0,
        };
        let total = cfg.super_blocks + 4 * cfg.zone_region_blocks;
        let ns = MemBlockDevice::new(256, total);

        let store = RegionZoneStore::open(Arc::new(ns.share()), &cfg, 1, true).unwrap();
        let log = store.liveness_log();
        assert_eq!(log.load(), None, "a fresh device has no watermark");

        log.store(1_000).unwrap();
        assert_eq!(log.load(), Some(1_000));
        let _z = store.open(5, Ordering::Unsorted).unwrap();
        assert_eq!(
            log.load(),
            Some(1_000),
            "allocating a zone must keep the watermark"
        );
        log.store(2_000).unwrap();
        assert_eq!(
            store.existing(),
            vec![5],
            "storing a watermark keeps regions"
        );

        drop(store);
        let reopened = RegionZoneStore::open(Arc::new(ns.share()), cfg, 1, false).unwrap();
        assert_eq!(
            reopened.liveness_log().load(),
            Some(2_000),
            "watermark durable across restart"
        );
        assert_eq!(
            reopened.existing(),
            vec![5],
            "regions durable across restart"
        );
    }

    #[test]
    fn region_zone_store_reopen_preserves_persisted_variable_lengths() {
        let initial = RegionZoneConfig {
            super_blocks: 8,
            zone_region_blocks: 64,
            zone_region_overrides: vec![(5, 32)],
            seg_blocks: 4,
            reserved_blocks: 0,
        };
        let changed = RegionZoneConfig {
            zone_region_overrides: vec![(5, 96), (6, 32), (7, 32)],
            ..initial.clone()
        };
        let backing = MemBlockDevice::new(256, 8 + 5 * 64);
        {
            let zs = RegionZoneStore::open(Arc::new(backing.share()), &initial, 1, true).unwrap();
            let mut z = zs.open(5, Ordering::Unsorted).unwrap();
            z.apply(b"secret", Some(b"value"), Version { hlc: 1, node: 1 })
                .unwrap();
            z.sync().unwrap();
            assert_eq!(zs.state.lock().unwrap().table.get(5).unwrap().len, 32);
        }
        {
            let zs = RegionZoneStore::open(Arc::new(backing.share()), &changed, 1, false).unwrap();
            assert_eq!(zs.state.lock().unwrap().table.get(5).unwrap().len, 32);
            assert_eq!(
                zs.open(5, Ordering::Unsorted)
                    .unwrap()
                    .get(b"secret")
                    .unwrap()
                    .as_deref(),
                Some(&b"value"[..])
            );
            let _z6 = zs.open(6, Ordering::Unsorted).unwrap();
            assert_eq!(zs.state.lock().unwrap().table.get(6).unwrap().len, 32);
            let old_base = zs.state.lock().unwrap().table.get(5).unwrap().base;
            zs.drop_zone(5).unwrap();
            let z7 = zs.open(7, Ordering::Unsorted).unwrap();
            assert_eq!(zs.state.lock().unwrap().table.get(7).unwrap().base, old_base);
            assert_eq!(z7.get(b"secret").unwrap(), None);
        }
        let reopened = RegionZoneStore::open(Arc::new(backing.share()), &initial, 1, false).unwrap();
        assert_eq!(reopened.existing(), vec![6, 7]);
        assert_eq!(reopened.open(7, Ordering::Unsorted).unwrap().get(b"secret").unwrap(), None);
    }

    #[test]
    fn mem_zone_store_isolates_zones() {
        let zs = MemZoneStore::new(1, small_geom());
        let mut a = zs.open(1, Ordering::Unsorted).unwrap();
        let b = zs.open(2, Ordering::Unsorted).unwrap();
        a.apply(b"k", Some(b"in-a"), Version { hlc: 10, node: 1 })
            .unwrap();
        a.sync().unwrap();

        assert_eq!(b.get(b"k").unwrap(), None);
        let a2 = zs.open(1, Ordering::Unsorted).unwrap();
        assert_eq!(a2.get(b"k").unwrap().as_deref(), Some(&b"in-a"[..]));

        assert_eq!(zs.existing(), vec![1, 2]);
        zs.drop_zone(1).unwrap();
        assert_eq!(zs.existing(), vec![2]);
        let a3 = zs.open(1, Ordering::Unsorted).unwrap();
        assert_eq!(a3.get(b"k").unwrap(), None);
    }

    fn temp_dir(tag: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, AtomicOrdering::Relaxed);
        std::env::temp_dir().join(format!("vals-zonestore-{}-{tag}-{n}", std::process::id()))
    }

    #[test]
    fn file_zone_store_persists_across_reopen() {
        let dir = temp_dir("persist");
        {
            let zs = FileZoneStore::new(1, &dir, small_geom()).unwrap();
            let mut z = zs.open(3, Ordering::Unsorted).unwrap();
            z.apply(b"k", Some(b"v"), Version { hlc: 5, node: 1 })
                .unwrap();
            z.sync().unwrap();
            assert_eq!(zs.existing(), vec![3]);
        }
        {
            let zs = FileZoneStore::new(1, &dir, small_geom()).unwrap();
            assert_eq!(zs.existing(), vec![3]);
            let z = zs.open(3, Ordering::Unsorted).unwrap();
            assert_eq!(z.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
            zs.drop_zone(3).unwrap();
            assert!(zs.existing().is_empty());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn subdev_dealloc_offsets_and_checks_all_ranges_before_discard() {
        let parent = Arc::new(MemBlockDevice::new(256, 100));
        parent.write_at(0, &vec![7; 256 * 100]).unwrap();
        let sub = SubDev::new(parent.clone(), 10, 20);
        assert!(sub.deallocate(&[(0, 1), (19, 2)]).is_err());
        let mut block = vec![0; 256];
        parent.read_at(10, &mut block).unwrap();
        assert_eq!(block, vec![7; 256]);
        assert!(sub.deallocate(&[(0, 1), (19, 1)]).unwrap());
        for (lba, byte) in [(9, 7), (10, 0), (11, 7), (29, 0), (30, 7)] {
            parent.read_at(lba, &mut block).unwrap();
            assert_eq!(block, vec![byte; 256]);
        }
    }

    #[test]
    fn subdev_offsets_reads_and_writes_and_bounds_checks() {
        let parent = Arc::new(MemBlockDevice::new(256, 100));
        let sub = SubDev::new(parent.clone(), 10, 20);
        assert_eq!(sub.num_blocks(), 20);
        assert_eq!(sub.block_size(), 256);

        let w = vec![7u8; 256];
        sub.write_at(0, &w).unwrap();
        let mut viap = vec![0u8; 256];
        parent.read_at(10, &mut viap).unwrap();
        assert_eq!(viap, w, "sub write landed at parent base + lba");
        let mut r = vec![0u8; 256];
        sub.read_at(0, &mut r).unwrap();
        assert_eq!(r, w);

        let mut one = vec![0u8; 256];
        sub.read_at(19, &mut one).unwrap();
        assert!(matches!(
            sub.read_at(20, &mut one),
            Err(BlockError::OutOfRange { .. })
        ));
        let mut two = vec![0u8; 512];
        assert!(
            matches!(
                sub.read_at(19, &mut two),
                Err(BlockError::OutOfRange { .. })
            ),
            "a read crossing the region end must be refused, not spill into a neighbor"
        );

        parent.write_at(11, &vec![3u8; 256]).unwrap();
        parent.write_at(12, &vec![4u8; 256]).unwrap();
        let (mut a, mut b) = (vec![0u8; 256], vec![0u8; 256]);
        let mut ops = vec![
            ReadOp {
                lba: 1,
                buf: &mut a,
            },
            ReadOp {
                lba: 2,
                buf: &mut b,
            },
        ];
        sub.read_batch(&mut ops).unwrap();
        assert_eq!(ops[0].lba, 1, "read_batch restored the caller's lba");
        assert_eq!(ops[1].lba, 2);
        assert!(a.iter().all(|&x| x == 3));
        assert!(b.iter().all(|&x| x == 4));
    }

    fn region_cfg() -> RegionZoneConfig {
        RegionZoneConfig {
            super_blocks: 8,
            zone_region_blocks: 64,
            zone_region_overrides: Vec::new(),
            seg_blocks: 4,
            reserved_blocks: 0,
        }
    }

    #[test]
    fn region_zone_store_isolates_zones_and_records_ordering() {
        let cfg = region_cfg();
        let total = cfg.super_blocks + 4 * cfg.zone_region_blocks;
        let parent = Arc::new(MemBlockDevice::new(256, total));
        let zs = RegionZoneStore::open(parent, cfg, 1, true).unwrap();

        let mut a = zs.open(0, Ordering::Unsorted).unwrap();
        let b = zs.open(5, Ordering::Unsorted).unwrap();
        a.apply(b"k", Some(b"in-a"), Version { hlc: 10, node: 1 })
            .unwrap();
        a.sync().unwrap();
        assert_eq!(b.get(b"k").unwrap(), None);
        assert_eq!(zs.existing(), vec![0, 5]);
        assert_eq!(zs.zone_ordering(5), Some(Ordering::Unsorted));
        let a2 = zs.open(0, Ordering::Unsorted).unwrap();
        assert_eq!(a2.get(b"k").unwrap().as_deref(), Some(&b"in-a"[..]));
    }

    #[test]
    fn region_zone_store_persists_across_a_reopen() {
        let cfg = region_cfg();
        let total = cfg.super_blocks + 4 * cfg.zone_region_blocks;
        let backing = MemBlockDevice::new(256, total);
        {
            let zs = RegionZoneStore::open(Arc::new(backing.share()), &cfg, 7, true).unwrap();
            let mut z = zs.open(3, Ordering::Sorted).unwrap();
            z.apply(b"k", Some(b"v"), Version { hlc: 5, node: 7 })
                .unwrap();
            z.sync().unwrap();
        }
        let zs = RegionZoneStore::open(Arc::new(backing.share()), cfg, 7, false).unwrap();
        assert_eq!(zs.existing(), vec![3]);
        assert_eq!(zs.zone_ordering(3), Some(Ordering::Sorted));
        let z = zs
            .open(3, Ordering::Unsorted)
            .unwrap();
        assert_eq!(z.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
    }

    #[test]
    fn superblock_carries_the_cluster_incarnation_only_once_bound() {
        let regions = [ZoneRegion { zone_id: 3, base: 8, len: 64, ordering: Ordering::Sorted }];
        let unbound = encode_superblock(4, &regions, Some(9), None, None);
        assert_eq!(u16::from_le_bytes(unbound[4..6].try_into().unwrap()), SUPER_VERSION_UNBOUND);
        assert_eq!(decode_superblock(&unbound).unwrap().unwrap().4, None);
        let id = 0xfeed_0000_0000_0000_0000_0000_0000_beefu128;
        let bound = encode_superblock(5, &regions, Some(9), None, Some(id));
        assert_eq!(u16::from_le_bytes(bound[4..6].try_into().unwrap()), 4);
        let (gen, dec, wm, topology, incarnation) = decode_superblock(&bound).unwrap().unwrap();
        assert_eq!((gen, dec, wm, topology, incarnation), (5, regions.to_vec(), Some(9), None, Some(id)));
        let mut bad = bound.clone();
        bad[29] ^= 1;
        assert!(decode_superblock(&bad).unwrap().is_none(), "the id is under the CRC");
    }

    #[test]
    fn region_zone_store_incarnation_survives_reopen_and_other_persists() {
        use crate::liveness::LivenessLog;
        let cfg = region_cfg();
        let total = cfg.super_blocks + 4 * cfg.zone_region_blocks;
        let backing = MemBlockDevice::new(256, total);
        let id = 0x1234_5678_9abc_def0_0fed_cba9_8765_4321u128;
        {
            let zs = RegionZoneStore::provision(Arc::new(backing.share()), &cfg, 7).unwrap();
            assert_eq!(zs.persisted_incarnation(), None);
            zs.persist_incarnation(Some(id)).unwrap();
            zs.liveness_log().store(5_000).unwrap();
            zs.open(4, Ordering::Sorted).unwrap();
        }
        {
            let zs = RegionZoneStore::open(Arc::new(backing.share()), &cfg, 7, false).unwrap();
            assert_eq!(zs.persisted_incarnation(), Some(id));
            assert_eq!(zs.liveness_log().load(), Some(5_000));
            assert_eq!(zs.existing(), vec![0, 4]);
            zs.liveness_log().store(6_000).unwrap();
            let slot_versions = |dev: &MemBlockDevice| {
                let mut slots = vec![0u8; 8 * 256];
                dev.read_at(0, &mut slots).unwrap();
                [0, 4 * 256].map(|at| u16::from_le_bytes([slots[at + 4], slots[at + 5]]))
            };
            assert_eq!(slot_versions(&backing), [4, 4], "both slots bound");
            zs.persist_incarnation(None).unwrap();
            assert_eq!(
                slot_versions(&backing),
                [SUPER_VERSION_UNBOUND; 2],
                "clearing rewrites both slots, so a pre-0283 reader opens the shard"
            );
        }
        let zs = RegionZoneStore::open(Arc::new(backing.share()), &cfg, 7, false).unwrap();
        assert_eq!(zs.persisted_incarnation(), None, "re-enrol --clear unbinds");
        assert_eq!(zs.liveness_log().load(), Some(6_000));
        zs.persist_incarnation(Some(id)).unwrap();
        drop(zs);
        let zs = RegionZoneStore::provision(Arc::new(backing.share()), &cfg, 7).unwrap();
        assert_eq!(zs.persisted_incarnation(), None, "provisioning starts unbound");
    }

    #[test]
    fn file_zone_store_incarnation_persists_and_a_damaged_file_fails_loud() {
        let dir = temp_dir("incarnation");
        let id = 0xabcdu128 << 64 | 0x77;
        {
            let zs = FileZoneStore::new(1, &dir, small_geom()).unwrap();
            assert_eq!(zs.persisted_incarnation(), None);
            zs.persist_incarnation(Some(id)).unwrap();
            assert!(zs.existing().is_empty(), "the id file is not a zone");
        }
        let zs = FileZoneStore::new(1, &dir, small_geom()).unwrap();
        assert_eq!(zs.persisted_incarnation(), Some(id));
        let path = dir.join("incarnation");
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[10] ^= 1;
        std::fs::write(&path, &bytes).unwrap();
        assert!(matches!(
            FileZoneStore::new(1, &dir, small_geom()),
            Err(ZoneStoreError::Corrupt(_))
        ));
        zs.persist_incarnation(None).unwrap();
        assert!(!path.exists());
        assert_eq!(FileZoneStore::new(1, &dir, small_geom()).unwrap().persisted_incarnation(), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn device_bytes(dev: &MemBlockDevice) -> Vec<u8> {
        let mut bytes = vec![0; dev.block_size() * dev.num_blocks() as usize];
        dev.read_at(0, &mut bytes).unwrap();
        bytes
    }

    #[test]
    fn provisioned_default_zone_survives_repeated_ordinary_restarts() {
        let cfg = region_cfg();
        let total = cfg.super_blocks + 2 * cfg.zone_region_blocks;
        let backing = MemBlockDevice::new(256, total);
        {
            let zs = RegionZoneStore::provision(Arc::new(backing.share()), &cfg, 7).unwrap();
            let mut zone0 = zs.open_existing(crate::zone::DEFAULT_ZONE).unwrap();
            zone0
                .apply(b"kept", Some(b"value"), Version { hlc: 5, node: 7 })
                .unwrap();
            zone0.sync().unwrap();
        }
        for _ in 0..2 {
            let zs = RegionZoneStore::open(Arc::new(backing.share()), &cfg, 7, false).unwrap();
            let zone0 = zs.open_existing(crate::zone::DEFAULT_ZONE).unwrap();
            assert_eq!(zone0.get(b"kept").unwrap().as_deref(), Some(&b"value"[..]));
        }
    }

    #[test]
    fn ordinary_start_refuses_missing_default_zone_without_writing() {
        let cfg = region_cfg();
        let total = cfg.super_blocks + 2 * cfg.zone_region_blocks;
        let backing = MemBlockDevice::new(256, total);
        RegionZoneStore::open(Arc::new(backing.share()), &cfg, 1, true).unwrap();
        let before = device_bytes(&backing);

        let reopened = RegionZoneStore::open(Arc::new(backing.share()), &cfg, 1, false).unwrap();
        let err = reopened.open_existing(crate::zone::DEFAULT_ZONE).err().unwrap();
        assert!(err.to_string().contains("valsd provision"), "{err}");
        assert_eq!(device_bytes(&backing), before);
    }

    #[test]
    fn invalid_provisioning_geometry_writes_nothing() {
        let cfg = region_cfg();
        let total = cfg.super_blocks + 2 * cfg.zone_region_blocks;
        let backing = MemBlockDevice::new(256, total);
        RegionZoneStore::provision(Arc::new(backing.share()), &cfg, 1).unwrap();
        let before = device_bytes(&backing);
        let invalid = RegionZoneConfig {
            zone_region_blocks: 3,
            ..cfg
        };

        assert!(RegionZoneStore::provision(Arc::new(backing.share()), invalid, 1).is_err());
        assert_eq!(device_bytes(&backing), before);
    }

    #[test]
    fn region_zone_store_reopen_of_a_blank_parent_is_refused() {
        let cfg = region_cfg();
        let total = cfg.super_blocks + 2 * cfg.zone_region_blocks;
        let backing = MemBlockDevice::new(256, total);
        let before = device_bytes(&backing);
        match RegionZoneStore::open(Arc::new(backing.share()), cfg, 1, false).map(|_| ()) {
            Err(ZoneStoreError::Corrupt(_)) => {}
            other => panic!("expected Corrupt for a blank reopen, got {other:?}"),
        }
        assert_eq!(device_bytes(&backing), before);
    }

    #[test]
    fn region_zone_store_drop_and_reuse_does_not_resurrect() {
        let cfg = region_cfg();
        let total = cfg.super_blocks + 2 * cfg.zone_region_blocks;
        let backing = MemBlockDevice::new(256, total);
        {
            let zs = RegionZoneStore::open(Arc::new(backing.share()), &cfg, 9, true).unwrap();
            let _z0 = zs.open(0, Ordering::Unsorted).unwrap();
            let mut z1 = zs.open(1, Ordering::Unsorted).unwrap();
            z1.apply(b"secret", Some(b"x"), Version { hlc: 1, node: 9 })
                .unwrap();
            z1.sync().unwrap();
            zs.drop_zone(1).unwrap();
            let z2 = zs.open(2, Ordering::Unsorted).unwrap();
            assert_eq!(
                z2.get(b"secret").unwrap(),
                None,
                "reused region must be wiped, not resurrect the dropped zone's data"
            );
        }
        let zs = RegionZoneStore::open(Arc::new(backing.share()), cfg, 9, false).unwrap();
        assert_eq!(zs.existing(), vec![0, 2]);
        let z2 = zs.open(2, Ordering::Unsorted).unwrap();
        assert_eq!(z2.get(b"secret").unwrap(), None);
    }

    #[test]
    fn region_zone_store_exhaustion_errors() {
        let cfg = region_cfg();
        let total = cfg.super_blocks + 2 * cfg.zone_region_blocks;
        let parent = Arc::new(MemBlockDevice::new(256, total));
        let zs = RegionZoneStore::open(parent, cfg, 1, true).unwrap();
        zs.open(0, Ordering::Unsorted).unwrap();
        zs.open(1, Ordering::Unsorted).unwrap();
        match zs.open(2, Ordering::Unsorted).map(|_| ()) {
            Err(ZoneStoreError::Exhausted { capacity_zones }) => assert_eq!(capacity_zones, 2),
            other => panic!("expected Exhausted, got {other:?}"),
        }
    }

    fn stamp_unknown_version_superblock(dev: &MemBlockDevice) {
        let mut buf = vec![0u8; dev.block_size()];
        buf[0..4].copy_from_slice(&SUPER_MAGIC.to_le_bytes());
        buf[4..6].copy_from_slice(&99u16.to_le_bytes());
        dev.write_at(0, &buf).unwrap();
        dev.flush().unwrap();
    }

    #[test]
    fn format_true_reformats_over_an_unknown_version_superblock() {
        let cfg = region_cfg();
        let total = cfg.super_blocks + 2 * cfg.zone_region_blocks;
        let backing = MemBlockDevice::new(256, total);
        stamp_unknown_version_superblock(&backing);

        let zs = RegionZoneStore::open(Arc::new(backing.share()), &cfg, 1, true).unwrap();
        assert!(zs.existing().is_empty(), "reformatted device has no zones");
        drop(zs);

        let reopened = RegionZoneStore::open(Arc::new(backing.share()), cfg, 1, false).unwrap();
        assert!(reopened.existing().is_empty());
    }

    #[test]
    fn format_true_reformats_over_a_valid_superblock_with_zones() {
        let cfg = region_cfg();
        let total = cfg.super_blocks + 4 * cfg.zone_region_blocks;
        let backing = MemBlockDevice::new(256, total);
        {
            let zs = RegionZoneStore::open(Arc::new(backing.share()), &cfg, 1, true).unwrap();
            let mut z = zs.open(3, Ordering::Unsorted).unwrap();
            z.apply(b"k", Some(b"v"), Version { hlc: 5, node: 1 }).unwrap();
            z.sync().unwrap();
            assert_eq!(zs.existing(), vec![3]);
        }
        let zs = RegionZoneStore::open(Arc::new(backing.share()), cfg, 1, true).unwrap();
        assert!(
            zs.existing().is_empty(),
            "format = true discards the prior region table"
        );
        let z = zs.open(3, Ordering::Unsorted).unwrap();
        assert_eq!(z.get(b"k").unwrap(), None, "prior data must not resurrect");
    }

    #[test]
    fn format_over_higher_generation_slot_does_not_resurrect_prior_table() {
        let cfg = region_cfg();
        let total = cfg.super_blocks + 2 * cfg.zone_region_blocks;
        let backing = MemBlockDevice::new(256, total);
        {
            let zs = RegionZoneStore::open(Arc::new(backing.share()), &cfg, 1, true).unwrap();
            let _z = zs.open(0, Ordering::Unsorted).unwrap();
            zs.drop_zone(0).unwrap();
        }
        {
            let zs = RegionZoneStore::open(Arc::new(backing.share()), &cfg, 1, true).unwrap();
            assert!(zs.existing().is_empty(), "reformat starts empty");
        }
        let reopened = RegionZoneStore::open(Arc::new(backing.share()), cfg, 1, false).unwrap();
        assert!(
            reopened.existing().is_empty(),
            "reformat must not be outranked by a stale higher-generation slot"
        );
    }

    #[test]
    fn reopen_over_unknown_version_superblock_errors_clearly() {
        let cfg = region_cfg();
        let total = cfg.super_blocks + 2 * cfg.zone_region_blocks;
        let backing = MemBlockDevice::new(256, total);
        stamp_unknown_version_superblock(&backing);

        match RegionZoneStore::open(Arc::new(backing.share()), cfg, 1, false) {
            Err(ZoneStoreError::Corrupt(msg)) => {
                assert!(
                    msg.contains("version") && msg.contains("valsd provision"),
                    "error should name the version problem and the remedy: {msg:?}"
                );
            }
            other => panic!("expected Corrupt(version), got {:?}", other.map(|_| ())),
        }
    }

    struct CountingShard {
        inner: MemBlockDevice,
        base: u64,
        len: u64,
        submits: std::sync::atomic::AtomicUsize,
    }

    impl BlockDevice for CountingShard {
        fn block_size(&self) -> usize {
            self.inner.block_size()
        }
        fn num_blocks(&self) -> u64 {
            self.len
        }
        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.inner.read_at(self.base + lba, buf)
        }
        fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
            self.inner.write_at(self.base + lba, buf)
        }
        fn flush(&self) -> vals_spdk::Result<()> {
            self.inner.flush()
        }
        fn submit_write(&self, lba: u64, buf: &[u8]) -> WriteHandle {
            self.submits
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            WriteHandle::ready(self.write_at(lba, buf))
        }
    }

    #[test]
    fn region_wipe_goes_through_the_async_submit_seam() {
        let cfg = Config {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: engine_ordering(Ordering::Unsorted),
        };
        let region_blocks = 64u64;
        let ns = MemBlockDevice::new(256, region_blocks);
        let dirty = vec![0xABu8; ns.block_size()];
        for b in 0..region_blocks {
            ns.write_at(b, &dirty).unwrap();
        }
        let dev = CountingShard {
            inner: ns.share(),
            base: 0,
            len: region_blocks,
            submits: std::sync::atomic::AtomicUsize::new(0),
        };
        wipe_region(&dev, &cfg).unwrap();
        let expected_segments = region_blocks / cfg.seg_blocks;
        assert_eq!(
            dev.submits.load(std::sync::atomic::Ordering::Relaxed) as u64,
            expected_segments,
            "one submit_write per segment header, through the async seam"
        );
        let engine = Engine::open(dev, cfg).unwrap();
        assert!(engine.is_empty(), "wiped region recovers as an empty engine");
    }
}
