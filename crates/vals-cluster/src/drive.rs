use std::io;

use vals_spdk::{BlockDevice, MemBlockDevice};
use vals_store::crc::crc32;
use vals_store::{Config, Engine, EngineError};

use crate::zone::Ordering;
use crate::zonestore::engine_ordering;

use crate::replica::VersionedStore;
use crate::ring::{hash64, NodeId};

const DRIVE_VNODES: u32 = 64;

#[derive(Debug)]
pub enum LayoutError {
    NotEnoughCapacity {
        partitions: usize,
        slots: usize,
    },
    Engine(EngineError),
    Io(io::Error),
    Corrupt(&'static str),
    LayoutMismatch,
    RelocateInProgress,
    NoSpareSlot,
}

impl std::fmt::Display for LayoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LayoutError::NotEnoughCapacity { partitions, slots } => write!(
                f,
                "drive set holds {slots} partition slots, need {partitions}"
            ),
            LayoutError::Engine(e) => write!(f, "{e}"),
            LayoutError::Io(e) => write!(f, "{e}"),
            LayoutError::Corrupt(w) => write!(f, "corrupt layout: {w}"),
            LayoutError::LayoutMismatch => write!(
                f,
                "recomputed layout does not match the persisted fingerprint \
                 (drive set changed shape — run an offline replan + relocate)"
            ),
            LayoutError::RelocateInProgress => write!(
                f,
                "a relocate is in progress (crash journal present) — re-run \
                 relocate to completion before reopening"
            ),
            LayoutError::NoSpareSlot => write!(
                f,
                "relocate needs a spare region to break a slot-swap cycle, but \
                 every drive slot is claimed by the old or new layout"
            ),
        }
    }
}
impl std::error::Error for LayoutError {}
impl From<EngineError> for LayoutError {
    fn from(e: EngineError) -> Self {
        LayoutError::Engine(e)
    }
}
impl From<io::Error> for LayoutError {
    fn from(e: io::Error) -> Self {
        LayoutError::Io(e)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Partition {
    pub partition: usize,
    pub drive: usize,
    pub base: u64,
    pub len: u64,
    pub core: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layout {
    partitions: Vec<Partition>,
    region_blocks: u64,
    num_cores: usize,
}

impl Layout {
    pub fn partitions(&self) -> &[Partition] {
        &self.partitions
    }

    pub fn num_partitions(&self) -> usize {
        self.partitions.len()
    }

    pub fn region_blocks(&self) -> u64 {
        self.region_blocks
    }

    pub fn partitions_on(&self, drive: usize) -> usize {
        self.partitions.iter().filter(|p| p.drive == drive).count()
    }

    pub fn fingerprint(&self) -> Vec<u8> {
        self.encode()
    }
}

fn drive_ring(caps: &[u64]) -> Vec<(u64, usize)> {
    let maxcap = caps.iter().copied().max().unwrap_or(1).max(1);
    let mut ring = Vec::new();
    for (d, &cap) in caps.iter().enumerate() {
        let vn = ((cap.saturating_mul(DRIVE_VNODES as u64)) / maxcap).max(1) as u32;
        for i in 0..vn {
            let mut buf = [0u8; 8];
            buf[0..4].copy_from_slice(&(d as u32).to_le_bytes());
            buf[4..8].copy_from_slice(&i.to_le_bytes());
            ring.push((hash64(&buf), d));
        }
    }
    ring.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    ring
}

fn home_drive(ring: &[(u64, usize)], partition: usize) -> usize {
    let pos = hash64(&(partition as u64).to_le_bytes());
    let start = ring.partition_point(|&(p, _)| p < pos) % ring.len();
    ring[start].1
}

pub fn plan(
    drive_caps: &[u64],
    region_blocks: u64,
    num_partitions: usize,
    num_cores: usize,
) -> Result<Layout, LayoutError> {
    assert!(region_blocks > 0, "region_blocks must be > 0");
    assert!(num_cores >= 1, "num_cores must be >= 1");
    assert!(!drive_caps.is_empty(), "a node needs at least one drive");

    let slots: Vec<usize> = drive_caps
        .iter()
        .map(|&c| (c / region_blocks) as usize)
        .collect();
    let total_slots: usize = slots.iter().sum();
    if total_slots < num_partitions {
        return Err(LayoutError::NotEnoughCapacity {
            partitions: num_partitions,
            slots: total_slots,
        });
    }

    let ring = drive_ring(drive_caps);
    let mut used = vec![0usize; drive_caps.len()];
    let mut partitions = Vec::with_capacity(num_partitions);
    for p in 0..num_partitions {
        let home = home_drive(&ring, p);
        let drive = if used[home] < slots[home] {
            home
        } else {
            (0..drive_caps.len())
                .map(|step| (home + step) % drive_caps.len())
                .find(|&d| used[d] < slots[d])
                .expect("total_slots >= num_partitions guarantees a free slot")
        };
        let base = used[drive] as u64 * region_blocks;
        used[drive] += 1;
        partitions.push(Partition {
            partition: p,
            drive,
            base,
            len: region_blocks,
            core: p % num_cores,
        });
    }
    Ok(Layout {
        partitions,
        region_blocks,
        num_cores,
    })
}

pub fn replan(
    old: &Layout,
    new_drive_caps: &[u64],
    num_cores: usize,
) -> Result<Layout, LayoutError> {
    plan(
        new_drive_caps,
        old.region_blocks,
        old.partitions.len(),
        num_cores,
    )
}

pub fn node_weight(num_drives: usize) -> u32 {
    num_drives.max(1) as u32
}

#[derive(Debug, Clone, Copy)]
pub struct PartitionGeom {
    pub seg_blocks: u64,
    pub reserved_blocks: u64,
    pub ordering: Ordering,
}

impl PartitionGeom {
    pub(crate) fn config(&self) -> Config {
        Config {
            seg_blocks: self.seg_blocks,
            reserved_blocks: self.reserved_blocks,
            ordering: engine_ordering(self.ordering),
        }
    }
}

pub trait DriveSet {
    type Dev: BlockDevice;
    fn drive_count(&self) -> usize;
    fn drive_blocks(&self, drive: usize) -> u64;
    fn open_region(
        &self,
        drive: usize,
        base: u64,
        len: u64,
        create: bool,
    ) -> Result<Self::Dev, io::Error>;

    fn read_layout_fingerprint(&self) -> Result<Option<Vec<u8>>, io::Error> {
        Ok(None)
    }

    fn write_layout_fingerprint(&self, _fp: &[u8]) -> Result<(), io::Error> {
        Ok(())
    }

    fn read_relocate_journal(&self) -> Result<Option<Vec<u8>>, io::Error> {
        Ok(None)
    }

    fn write_relocate_journal(&self, _j: &[u8]) -> Result<(), io::Error> {
        Ok(())
    }

    fn clear_relocate_journal(&self) -> Result<(), io::Error> {
        Ok(())
    }
}

pub fn verify_layout<DS: DriveSet>(
    ds: &DS,
    layout: &Layout,
    create: bool,
) -> Result<(), LayoutError> {
    if ds.read_relocate_journal()?.is_some() {
        return Err(LayoutError::RelocateInProgress);
    }
    let fp = layout.fingerprint();
    if create {
        ds.write_layout_fingerprint(&fp)?;
    } else if let Some(stored) = ds.read_layout_fingerprint()? {
        if stored != fp {
            return Err(LayoutError::LayoutMismatch);
        }
    }
    Ok(())
}

pub fn drive_caps<DS: DriveSet>(ds: &DS) -> Vec<u64> {
    (0..ds.drive_count()).map(|d| ds.drive_blocks(d)).collect()
}

fn open_partition<DS: DriveSet>(
    ds: &DS,
    p: &Partition,
    geom: PartitionGeom,
    node: NodeId,
    create: bool,
) -> Result<VersionedStore<DS::Dev>, LayoutError> {
    let dev = ds.open_region(p.drive, p.base, p.len, create)?;
    let store = if create {
        VersionedStore::new(Engine::create(dev, geom.config())?, node)?
    } else {
        VersionedStore::open(dev, geom.config(), node)?
    };
    Ok(store)
}

pub fn build_stores<DS: DriveSet>(
    ds: &DS,
    layout: &Layout,
    geom: PartitionGeom,
    node: NodeId,
    create: bool,
) -> Result<Vec<VersionedStore<DS::Dev>>, LayoutError> {
    build_stores_with(ds, layout, geom, node, |_| create)
}

pub fn build_stores_with<DS, F>(
    ds: &DS,
    layout: &Layout,
    geom: PartitionGeom,
    node: NodeId,
    create_for: F,
) -> Result<Vec<VersionedStore<DS::Dev>>, LayoutError>
where
    DS: DriveSet,
    F: Fn(usize) -> bool,
{
    layout
        .partitions
        .iter()
        .map(|p| open_partition(ds, p, geom, node, create_for(p.partition)))
        .collect()
}

pub fn relocate<DS: DriveSet>(
    ds: &DS,
    old: &Layout,
    new: &Layout,
    geom: PartitionGeom,
    node: NodeId,
) -> Result<usize, LayoutError> {
    relocate_with(ds, old, new, |src, dst, _commit| {
        stream_partition(ds, src, dst, geom, node)
    })
}

pub fn relocate_with<DS, M>(
    ds: &DS,
    old: &Layout,
    new: &Layout,
    mut mover: M,
) -> Result<usize, LayoutError>
where
    DS: DriveSet,
    M: FnMut(
        &Partition,
        &Partition,
        &mut dyn FnMut() -> Result<(), LayoutError>,
    ) -> Result<(), LayoutError>,
{
    assert_eq!(
        old.partitions.len(),
        new.partitions.len(),
        "partition count is fixed across a relayout"
    );
    assert_eq!(
        old.region_blocks, new.region_blocks,
        "region size is fixed across a relayout"
    );
    let region = old.region_blocks;

    let moves: Vec<Move> = old
        .partitions
        .iter()
        .zip(&new.partitions)
        .filter(|(po, pn)| (po.drive, po.base) != (pn.drive, pn.base))
        .map(|(po, pn)| Move { src: *po, dst: *pn })
        .collect();

    if moves.is_empty() {
        ds.write_layout_fingerprint(&new.fingerprint())?;
        return Ok(0);
    }

    let mut statuses: Vec<MoveStatus> = match ds.read_relocate_journal()? {
        Some(bytes) => {
            let j = RelocateJournal::decode(&bytes)
                .ok_or(LayoutError::Corrupt("relocate journal failed to decode"))?;
            if j.old != old.encode() || j.new != new.encode() {
                return Err(LayoutError::Corrupt(
                    "relocate journal is for a different relayout",
                ));
            }
            if j.statuses.len() != moves.len() {
                return Err(LayoutError::Corrupt("relocate journal move-count mismatch"));
            }
            j.statuses
        }
        None => {
            let statuses = vec![MoveStatus::Pending; moves.len()];
            persist_journal(ds, old, new, &statuses)?;
            statuses
        }
    };
    let moved = moves.len();

    loop {
        let mut progressed = false;
        let mut remaining = false;
        for i in 0..moves.len() {
            if matches!(statuses[i], MoveStatus::Done) {
                continue;
            }
            remaining = true;
            let dst_region = (moves[i].dst.drive, moves[i].dst.base);
            if dst_is_pending_source(&moves, &statuses, dst_region) {
                continue;
            }
            let src = match statuses[i] {
                MoveStatus::Staged { drive, base } => Partition {
                    partition: usize::MAX,
                    drive,
                    base,
                    len: region,
                    core: 0,
                },
                _ => moves[i].src,
            };
            let mut committed = statuses.clone();
            committed[i] = MoveStatus::Done;
            mover(&src, &moves[i].dst, &mut || {
                persist_journal(ds, old, new, &committed)
            })?;
            statuses[i] = MoveStatus::Done;
            persist_journal(ds, old, new, &statuses)?;
            progressed = true;
        }
        if !remaining {
            break;
        }
        if !progressed {
            let i = (0..moves.len())
                .find(|&i| matches!(statuses[i], MoveStatus::Pending))
                .expect("blocked progress implies a pending move to stage");
            let temp = find_spare_slot(ds, old, new, region)?;
            let staged = MoveStatus::Staged {
                drive: temp.drive,
                base: temp.base,
            };
            let mut committed = statuses.clone();
            committed[i] = staged;
            mover(&moves[i].src, &temp, &mut || {
                persist_journal(ds, old, new, &committed)
            })?;
            statuses[i] = staged;
            persist_journal(ds, old, new, &statuses)?;
        }
    }

    ds.write_layout_fingerprint(&new.fingerprint())?;
    ds.clear_relocate_journal()?;
    Ok(moved)
}

#[derive(Clone, Copy)]
struct Move {
    src: Partition,
    dst: Partition,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MoveStatus {
    Pending,
    Staged { drive: usize, base: u64 },
    Done,
}

fn dst_is_pending_source(moves: &[Move], statuses: &[MoveStatus], region: (usize, u64)) -> bool {
    moves
        .iter()
        .zip(statuses)
        .any(|(m, s)| matches!(s, MoveStatus::Pending) && (m.src.drive, m.src.base) == region)
}

fn stream_partition<DS: DriveSet>(
    ds: &DS,
    src: &Partition,
    dst: &Partition,
    geom: PartitionGeom,
    node: NodeId,
) -> Result<(), LayoutError> {
    let mut dst_store = open_partition(ds, dst, geom, node, true)?;
    if let Ok(src_store) = open_partition(ds, src, geom, node, false) {
        for key in src_store.keys()? {
            if let Some(stored) = src_store.stored(&key)? {
                dst_store.apply(&key, stored.value.as_deref(), stored.version)?;
            }
        }
    }
    dst_store.sync()?;
    Ok(())
}

fn find_spare_slot<DS: DriveSet>(
    ds: &DS,
    old: &Layout,
    new: &Layout,
    region: u64,
) -> Result<Partition, LayoutError> {
    let claimed: std::collections::HashSet<(usize, u64)> = old
        .partitions
        .iter()
        .chain(&new.partitions)
        .map(|p| (p.drive, p.base))
        .collect();
    for d in 0..ds.drive_count() {
        let slots = ds.drive_blocks(d) / region;
        for s in 0..slots {
            let base = s * region;
            if !claimed.contains(&(d, base)) {
                return Ok(Partition {
                    partition: usize::MAX,
                    drive: d,
                    base,
                    len: region,
                    core: 0,
                });
            }
        }
    }
    Err(LayoutError::NoSpareSlot)
}

fn persist_journal<DS: DriveSet>(
    ds: &DS,
    old: &Layout,
    new: &Layout,
    statuses: &[MoveStatus],
) -> Result<(), LayoutError> {
    let j = RelocateJournal {
        old: old.encode(),
        new: new.encode(),
        statuses: statuses.to_vec(),
    };
    ds.write_relocate_journal(&j.encode())?;
    Ok(())
}

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct MemDriveSet {
    inner: Arc<MemDriveSetInner>,
}

struct MemDriveSetInner {
    block_size: usize,
    caps: Mutex<Vec<u64>>,
    devices: Mutex<HashMap<(usize, u64), MemBlockDevice>>,
    fingerprint: Mutex<Option<Vec<u8>>>,
    journal: Mutex<Option<Vec<u8>>>,
}

impl MemDriveSet {
    pub fn uniform(block_size: usize, drive_blocks: u64, drives: usize) -> Self {
        Self::with_caps(block_size, vec![drive_blocks; drives])
    }

    pub fn with_caps(block_size: usize, caps: Vec<u64>) -> Self {
        MemDriveSet {
            inner: Arc::new(MemDriveSetInner {
                block_size,
                caps: Mutex::new(caps),
                devices: Mutex::new(HashMap::new()),
                fingerprint: Mutex::new(None),
                journal: Mutex::new(None),
            }),
        }
    }

    pub fn share(&self) -> MemDriveSet {
        MemDriveSet {
            inner: self.inner.clone(),
        }
    }

    pub fn set_cap(&self, drive: usize, blocks: u64) {
        self.inner.caps.lock().unwrap()[drive] = blocks;
    }

    pub fn fail_drive(&mut self, drive: usize) {
        self.inner.caps.lock().unwrap()[drive] = 0;
        self.inner
            .devices
            .lock()
            .unwrap()
            .retain(|&(d, _), _| d != drive);
    }

    pub fn replace_drive(&mut self, drive: usize, blocks: u64) {
        self.inner
            .devices
            .lock()
            .unwrap()
            .retain(|&(d, _), _| d != drive);
        self.inner.caps.lock().unwrap()[drive] = blocks;
    }
}

impl DriveSet for MemDriveSet {
    type Dev = MemBlockDevice;
    fn drive_count(&self) -> usize {
        self.inner.caps.lock().unwrap().len()
    }
    fn drive_blocks(&self, drive: usize) -> u64 {
        self.inner.caps.lock().unwrap()[drive]
    }
    fn read_layout_fingerprint(&self) -> Result<Option<Vec<u8>>, io::Error> {
        Ok(self.inner.fingerprint.lock().unwrap().clone())
    }
    fn write_layout_fingerprint(&self, fp: &[u8]) -> Result<(), io::Error> {
        *self.inner.fingerprint.lock().unwrap() = Some(fp.to_vec());
        Ok(())
    }
    fn read_relocate_journal(&self) -> Result<Option<Vec<u8>>, io::Error> {
        Ok(self.inner.journal.lock().unwrap().clone())
    }
    fn write_relocate_journal(&self, j: &[u8]) -> Result<(), io::Error> {
        *self.inner.journal.lock().unwrap() = Some(j.to_vec());
        Ok(())
    }
    fn clear_relocate_journal(&self) -> Result<(), io::Error> {
        *self.inner.journal.lock().unwrap() = None;
        Ok(())
    }
    fn open_region(
        &self,
        drive: usize,
        base: u64,
        len: u64,
        create: bool,
    ) -> Result<MemBlockDevice, io::Error> {
        let mut devs = self.inner.devices.lock().unwrap();
        match devs.get(&(drive, base)) {
            Some(dev) if !create => Ok(dev.share()),
            _ => {
                let dev = MemBlockDevice::new(self.inner.block_size, len);
                devs.insert((drive, base), dev.share());
                Ok(dev)
            }
        }
    }
}

#[cfg(feature = "spdk")]
pub struct SpdkDriveSet {
    controllers: Vec<std::sync::Arc<vals_spdk::spdk::SpdkController>>,
}

#[cfg(feature = "spdk")]
impl SpdkDriveSet {
    pub fn attach(pci_addrs: &[String]) -> Result<Self, LayoutError> {
        let mut controllers = Vec::with_capacity(pci_addrs.len());
        for addr in pci_addrs {
            let ctrlr = vals_spdk::spdk::SpdkController::attach(&vals_spdk::spdk::SpdkConfig {
                pci_addr: addr.clone(),
                nsid: 1,
            })
            .map_err(|e| LayoutError::Io(io::Error::other(e.to_string())))?;
            controllers.push(ctrlr);
        }
        Ok(SpdkDriveSet { controllers })
    }

    pub fn from_controllers(
        controllers: Vec<std::sync::Arc<vals_spdk::spdk::SpdkController>>,
    ) -> Self {
        SpdkDriveSet { controllers }
    }
}

#[cfg(feature = "spdk")]
impl DriveSet for SpdkDriveSet {
    type Dev = vals_spdk::spdk::SpdkBlockDevice;
    fn drive_count(&self) -> usize {
        self.controllers.len()
    }
    fn drive_blocks(&self, drive: usize) -> u64 {
        self.controllers[drive].num_blocks()
    }
    fn open_region(
        &self,
        drive: usize,
        base: u64,
        len: u64,
        _create: bool,
    ) -> Result<Self::Dev, io::Error> {
        self.controllers[drive]
            .open_shard(base, len)
            .map_err(|e| io::Error::other(e.to_string()))
    }
}

const LAYOUT_MAGIC: u32 = 0x564c_444d;
const LAYOUT_VERSION: u16 = 1;
const LAYOUT_HEADER_BYTES: usize = 4 + 2 + 8 + 4 + 4;
const PART_ENTRY_BYTES: usize = 4 + 4 + 8 + 8 + 4;

impl Layout {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf =
            Vec::with_capacity(LAYOUT_HEADER_BYTES + self.partitions.len() * PART_ENTRY_BYTES + 4);
        buf.extend_from_slice(&LAYOUT_MAGIC.to_le_bytes());
        buf.extend_from_slice(&LAYOUT_VERSION.to_le_bytes());
        buf.extend_from_slice(&self.region_blocks.to_le_bytes());
        buf.extend_from_slice(&(self.num_cores as u32).to_le_bytes());
        buf.extend_from_slice(&(self.partitions.len() as u32).to_le_bytes());
        for p in &self.partitions {
            buf.extend_from_slice(&(p.partition as u32).to_le_bytes());
            buf.extend_from_slice(&(p.drive as u32).to_le_bytes());
            buf.extend_from_slice(&p.base.to_le_bytes());
            buf.extend_from_slice(&p.len.to_le_bytes());
            buf.extend_from_slice(&(p.core as u32).to_le_bytes());
        }
        let crc = crc32(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    pub fn decode(buf: &[u8]) -> Option<Layout> {
        if buf.len() < LAYOUT_HEADER_BYTES {
            return None;
        }
        if u32::from_le_bytes(buf[0..4].try_into().ok()?) != LAYOUT_MAGIC {
            return None;
        }
        if u16::from_le_bytes(buf[4..6].try_into().ok()?) != LAYOUT_VERSION {
            return None;
        }
        let region_blocks = u64::from_le_bytes(buf[6..14].try_into().ok()?);
        let num_cores = u32::from_le_bytes(buf[14..18].try_into().ok()?) as usize;
        let count = u32::from_le_bytes(buf[18..22].try_into().ok()?) as usize;
        let body_end = LAYOUT_HEADER_BYTES + count * PART_ENTRY_BYTES;
        if buf.len() < body_end + 4 {
            return None;
        }
        if crc32(&buf[..body_end])
            != u32::from_le_bytes(buf[body_end..body_end + 4].try_into().ok()?)
        {
            return None;
        }
        let mut partitions = Vec::with_capacity(count);
        let mut off = LAYOUT_HEADER_BYTES;
        for _ in 0..count {
            partitions.push(Partition {
                partition: u32::from_le_bytes(buf[off..off + 4].try_into().ok()?) as usize,
                drive: u32::from_le_bytes(buf[off + 4..off + 8].try_into().ok()?) as usize,
                base: u64::from_le_bytes(buf[off + 8..off + 16].try_into().ok()?),
                len: u64::from_le_bytes(buf[off + 16..off + 24].try_into().ok()?),
                core: u32::from_le_bytes(buf[off + 24..off + 28].try_into().ok()?) as usize,
            });
            off += PART_ENTRY_BYTES;
        }
        Some(Layout {
            partitions,
            region_blocks,
            num_cores,
        })
    }
}

const JOURNAL_MAGIC: u32 = 0x564c_4a52;
const JOURNAL_VERSION: u16 = 1;

struct RelocateJournal {
    old: Vec<u8>,
    new: Vec<u8>,
    statuses: Vec<MoveStatus>,
}

impl RelocateJournal {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&JOURNAL_MAGIC.to_le_bytes());
        buf.extend_from_slice(&JOURNAL_VERSION.to_le_bytes());
        buf.extend_from_slice(&(self.old.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.old);
        buf.extend_from_slice(&(self.new.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.new);
        buf.extend_from_slice(&(self.statuses.len() as u32).to_le_bytes());
        for s in &self.statuses {
            let (tag, drive, base) = match *s {
                MoveStatus::Pending => (0u8, 0u32, 0u64),
                MoveStatus::Staged { drive, base } => (1u8, drive as u32, base),
                MoveStatus::Done => (2u8, 0u32, 0u64),
            };
            buf.push(tag);
            buf.extend_from_slice(&drive.to_le_bytes());
            buf.extend_from_slice(&base.to_le_bytes());
        }
        let crc = crc32(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    fn decode(buf: &[u8]) -> Option<RelocateJournal> {
        let mut off = 0usize;
        let take = |off: &mut usize, n: usize| -> Option<&[u8]> {
            let s = buf.get(*off..*off + n)?;
            *off += n;
            Some(s)
        };
        if u32::from_le_bytes(take(&mut off, 4)?.try_into().ok()?) != JOURNAL_MAGIC {
            return None;
        }
        if u16::from_le_bytes(take(&mut off, 2)?.try_into().ok()?) != JOURNAL_VERSION {
            return None;
        }
        let old_len = u32::from_le_bytes(take(&mut off, 4)?.try_into().ok()?) as usize;
        let old = take(&mut off, old_len)?.to_vec();
        let new_len = u32::from_le_bytes(take(&mut off, 4)?.try_into().ok()?) as usize;
        let new = take(&mut off, new_len)?.to_vec();
        let count = u32::from_le_bytes(take(&mut off, 4)?.try_into().ok()?) as usize;
        let mut statuses = Vec::with_capacity(count);
        for _ in 0..count {
            let tag = take(&mut off, 1)?[0];
            let drive = u32::from_le_bytes(take(&mut off, 4)?.try_into().ok()?) as usize;
            let base = u64::from_le_bytes(take(&mut off, 8)?.try_into().ok()?);
            statuses.push(match tag {
                0 => MoveStatus::Pending,
                1 => MoveStatus::Staged { drive, base },
                2 => MoveStatus::Done,
                _ => return None,
            });
        }
        let body_end = off;
        let crc = u32::from_le_bytes(take(&mut off, 4)?.try_into().ok()?);
        if crc != crc32(&buf[..body_end]) {
            return None;
        }
        Some(RelocateJournal { old, new, statuses })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::Version;

    fn geom() -> PartitionGeom {
        PartitionGeom {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        }
    }

    #[test]
    fn plan_places_every_partition_and_is_deterministic() {
        let caps = vec![1024u64; 4];
        let a = plan(&caps, 64, 16, 8).unwrap();
        let b = plan(&caps, 64, 16, 8).unwrap();
        assert_eq!(a, b, "plan is a pure function of its inputs");
        assert_eq!(a.num_partitions(), 16);
        for (i, p) in a.partitions().iter().enumerate() {
            assert_eq!(p.partition, i);
            assert!(p.drive < 4);
            assert_eq!(p.core, i % 8);
            assert!(p.base + p.len <= caps[p.drive]);
        }
        for d in 0..4 {
            let mut bases: Vec<u64> = a
                .partitions()
                .iter()
                .filter(|p| p.drive == d)
                .map(|p| p.base)
                .collect();
            bases.sort_unstable();
            for w in bases.windows(2) {
                assert!(w[1] >= w[0] + 64, "regions overlap on drive {d}");
            }
        }
    }

    #[test]
    fn partitions_spread_across_drives() {
        let caps = vec![4096u64; 4];
        let layout = plan(&caps, 64, 32, 16).unwrap();
        for d in 0..4 {
            let n = layout.partitions_on(d);
            assert!(n >= 3, "drive {d} under-used: {n} of 32 across 4 drives");
        }
    }

    #[test]
    fn bigger_drives_host_more_partitions() {
        let caps = vec![8192u64, 2048, 2048, 2048];
        let layout = plan(&caps, 64, 40, 16).unwrap();
        let big = layout.partitions_on(0);
        let small: usize = (1..4).map(|d| layout.partitions_on(d)).sum();
        assert!(
            big > small / 3,
            "the 4× drive should host a large share: big={big}, others={small}"
        );
    }

    #[test]
    fn not_enough_capacity_is_an_error() {
        let caps = vec![128u64, 128];
        match plan(&caps, 64, 5, 4) {
            Err(LayoutError::NotEnoughCapacity { partitions, slots }) => {
                assert_eq!((partitions, slots), (5, 4));
            }
            other => panic!("expected NotEnoughCapacity, got {other:?}"),
        }
    }

    #[test]
    fn build_then_recover_reads_back() {
        let ds = MemDriveSet::uniform(256, 1024, 3);
        let caps = drive_caps(&ds);
        let layout = plan(&caps, 64, 6, 4).unwrap();
        let mut stores = build_stores(&ds, &layout, geom(), 7, true).unwrap();
        for (i, s) in stores.iter_mut().enumerate() {
            let k = format!("p{i}");
            s.apply(k.as_bytes(), Some(b"v"), Version { hlc: 1, node: 7 })
                .unwrap();
            s.sync().unwrap();
        }
        drop(stores);
        let recovered = plan(&caps, 64, 6, 4).unwrap();
        assert_eq!(recovered, layout);
        let stores = build_stores(&ds, &recovered, geom(), 7, false).unwrap();
        for (i, s) in stores.iter().enumerate() {
            let k = format!("p{i}");
            assert_eq!(s.get(k.as_bytes()).unwrap().as_deref(), Some(&b"v"[..]));
        }
    }

    #[test]
    fn relocate_moves_only_changed_partitions_and_preserves_data() {
        let ds = MemDriveSet::uniform(256, 1024, 4);
        let caps3 = vec![1024u64, 1024, 1024, 0];
        let layout3 = plan(&caps3, 64, 12, 8).unwrap();
        let mut stores = build_stores(&ds, &layout3, geom(), 9, true).unwrap();
        for (i, s) in stores.iter_mut().enumerate() {
            s.apply(
                format!("k{i}").as_bytes(),
                Some(b"v"),
                Version { hlc: 1, node: 9 },
            )
            .unwrap();
            s.sync().unwrap();
        }
        drop(stores);

        ds.set_cap(3, 1024);
        let caps4 = drive_caps(&ds);
        let layout4 = replan(&layout3, &caps4, 8).unwrap();

        let moved = relocate(&ds, &layout3, &layout4, geom(), 9).unwrap();
        assert!(
            moved > 0 && moved < 12,
            "bounded movement: {moved}/12 moved"
        );
        assert!(
            layout4.partitions_on(3) > 0,
            "the new drive should host partitions"
        );

        let stores = build_stores(&ds, &layout4, geom(), 9, false).unwrap();
        for (i, s) in stores.iter().enumerate() {
            assert_eq!(
                s.get(format!("k{i}").as_bytes()).unwrap().as_deref(),
                Some(&b"v"[..]),
                "partition {i} lost data across relocate"
            );
        }
    }

    #[test]
    fn drive_failure_relocates_lost_partitions_empty() {
        let mut ds = MemDriveSet::uniform(256, 1024, 3);
        let caps = drive_caps(&ds);
        let layout = plan(&caps, 64, 9, 8).unwrap();
        let mut stores = build_stores(&ds, &layout, geom(), 5, true).unwrap();
        for (i, s) in stores.iter_mut().enumerate() {
            s.apply(
                format!("k{i}").as_bytes(),
                Some(b"v"),
                Version { hlc: 1, node: 5 },
            )
            .unwrap();
            s.sync().unwrap();
        }
        let lost: Vec<usize> = layout
            .partitions()
            .iter()
            .filter(|p| p.drive == 1)
            .map(|p| p.partition)
            .collect();
        assert!(!lost.is_empty());
        drop(stores);

        ds.fail_drive(1);
        let caps2 = drive_caps(&ds);
        let layout2 = replan(&layout, &caps2, 8).unwrap();
        relocate(&ds, &layout, &layout2, geom(), 5).unwrap();

        assert_eq!(layout2.partitions_on(1), 0);
        let stores = build_stores(&ds, &layout2, geom(), 5, false).unwrap();
        for (i, s) in stores.iter().enumerate() {
            let got = s.get(format!("k{i}").as_bytes()).unwrap();
            if lost.contains(&i) {
                assert_eq!(got, None, "lost partition {i} should be empty post-failure");
            } else {
                assert_eq!(
                    got.as_deref(),
                    Some(&b"v"[..]),
                    "survivor {i} kept its data"
                );
            }
        }
    }

    #[test]
    fn verify_layout_persists_then_accepts_an_unchanged_reopen() {
        let ds = MemDriveSet::uniform(256, 1024, 3);
        let caps = drive_caps(&ds);
        let layout = plan(&caps, 64, 6, 4).unwrap();
        verify_layout(&ds, &layout, true).unwrap();
        let reopened = plan(&drive_caps(&ds), 64, 6, 4).unwrap();
        assert_eq!(reopened, layout);
        verify_layout(&ds, &reopened, false).unwrap();
    }

    #[test]
    fn verify_layout_refuses_a_reopen_after_a_capacity_change() {
        let ds = MemDriveSet::uniform(256, 1024, 3);
        let layout = plan(&drive_caps(&ds), 64, 6, 4).unwrap();
        verify_layout(&ds, &layout, true).unwrap();

        ds.set_cap(1, 4096);
        let recomputed = plan(&drive_caps(&ds), 64, 6, 4).unwrap();
        assert_ne!(recomputed, layout, "capacity change moved partitions");
        match verify_layout(&ds, &recomputed, false) {
            Err(LayoutError::LayoutMismatch) => {}
            other => panic!("expected LayoutMismatch, got {other:?}"),
        }
        let new_layout = replan(&layout, &drive_caps(&ds), 4).unwrap();
        assert_eq!(new_layout, recomputed);
        relocate(&ds, &layout, &new_layout, geom(), 1).unwrap();
        verify_layout(&ds, &new_layout, false).unwrap();
    }

    #[test]
    fn offline_rebalance_reopens_without_manual_fingerprint_repair() {
        let ds = MemDriveSet::uniform(256, 1024, 3);
        let layout = plan(&drive_caps(&ds), 64, 6, 4).unwrap();
        verify_layout(&ds, &layout, true).unwrap();
        let mut stores = build_stores(&ds, &layout, geom(), 3, true).unwrap();
        for (i, s) in stores.iter_mut().enumerate() {
            s.apply(
                format!("k{i}").as_bytes(),
                Some(b"v"),
                Version { hlc: 1, node: 3 },
            )
            .unwrap();
            s.sync().unwrap();
        }
        drop(stores);

        ds.set_cap(1, 4096);
        let new_layout = replan(&layout, &drive_caps(&ds), 4).unwrap();
        assert_ne!(new_layout, layout, "capacity change moved partitions");
        relocate(&ds, &layout, &new_layout, geom(), 3).unwrap();

        verify_layout(&ds, &new_layout, false).unwrap();
        let stores = build_stores(&ds, &new_layout, geom(), 3, false).unwrap();
        for (i, s) in stores.iter().enumerate() {
            assert_eq!(
                s.get(format!("k{i}").as_bytes()).unwrap().as_deref(),
                Some(&b"v"[..]),
                "partition {i} lost data across the rebalance"
            );
        }
    }

    #[test]
    fn verify_layout_allows_an_unchanged_cold_swap() {
        let mut ds = MemDriveSet::uniform(256, 1024, 3);
        let layout = plan(&drive_caps(&ds), 64, 6, 4).unwrap();
        verify_layout(&ds, &layout, true).unwrap();
        ds.replace_drive(1, 1024);
        let recomputed = plan(&drive_caps(&ds), 64, 6, 4).unwrap();
        assert_eq!(recomputed, layout);
        verify_layout(&ds, &recomputed, false).unwrap();
    }

    #[test]
    fn verify_layout_is_inert_without_a_persisted_fingerprint() {
        struct NoFpDriveSet(MemDriveSet);
        impl DriveSet for NoFpDriveSet {
            type Dev = MemBlockDevice;
            fn drive_count(&self) -> usize {
                self.0.drive_count()
            }
            fn drive_blocks(&self, d: usize) -> u64 {
                self.0.drive_blocks(d)
            }
            fn open_region(
                &self,
                drive: usize,
                base: u64,
                len: u64,
                create: bool,
            ) -> Result<MemBlockDevice, io::Error> {
                self.0.open_region(drive, base, len, create)
            }
        }
        let ds = NoFpDriveSet(MemDriveSet::uniform(256, 1024, 3));
        let layout = plan(&drive_caps(&ds), 64, 6, 4).unwrap();
        verify_layout(&ds, &layout, true).unwrap();
        let other = plan(&[2048u64, 2048, 2048], 64, 6, 4).unwrap();
        verify_layout(&ds, &other, false).unwrap();
    }

    #[test]
    fn node_weight_tracks_drive_count() {
        assert_eq!(node_weight(8), 8);
        assert_eq!(node_weight(1), 1);
        assert_eq!(node_weight(0), 1);
    }

    #[test]
    fn layout_codec_roundtrips_and_rejects_corruption() {
        let layout = plan(&[1024u64; 3], 64, 7, 4).unwrap();
        let enc = layout.encode();
        assert_eq!(Layout::decode(&enc).unwrap(), layout);

        let mut bad = enc.clone();
        bad[0] ^= 0xFF;
        assert!(Layout::decode(&bad).is_none(), "bad magic");
        let mut bad = enc.clone();
        bad[24] ^= 0x01;
        assert!(Layout::decode(&bad).is_none(), "crc mismatch");
        assert!(Layout::decode(&enc[..enc.len() - 3]).is_none(), "truncated");

        let mut future = enc.clone();
        future[4..6].copy_from_slice(&(LAYOUT_VERSION + 1).to_le_bytes());
        let body_end = future.len() - 4;
        let crc = crc32(&future[..body_end]);
        future[body_end..].copy_from_slice(&crc.to_le_bytes());
        assert!(Layout::decode(&future).is_none(), "unknown version");
    }

    #[test]
    fn three_node_two_drive_cluster_survives_drive_failure_and_replacement() {
        use crate::antientropy::{sweep, LocalPeer, DEFAULT_BUCKETS};
        use crate::ring::{hash64, Ring};
        use std::cell::RefCell;

        const P: usize = 4;
        const RF: usize = 2;
        const REGION: u64 = 256;
        const DRIVE_BLOCKS: u64 = 1024;

        let mut ds: Vec<MemDriveSet> = (0..3)
            .map(|_| MemDriveSet::uniform(256, DRIVE_BLOCKS, 2))
            .collect();
        let caps = drive_caps(&ds[0]);
        let layout = plan(&caps, REGION, P, P).unwrap();

        type Stores = Vec<RefCell<VersionedStore<MemBlockDevice>>>;
        let build = |ds: &MemDriveSet, node: u32, create: bool| -> Stores {
            build_stores(ds, &layout, geom(), node, create)
                .unwrap()
                .into_iter()
                .map(RefCell::new)
                .collect()
        };
        let mut nodes: Vec<Stores> = (0..3)
            .map(|i| build(&ds[i], (i + 1) as u32, true))
            .collect();

        let ring = Ring::from_members(RF, 64, [1, 2, 3]);
        let part_of = |k: &[u8]| (hash64(k) % P as u64) as usize;
        let present = |nodes: &Vec<Stores>, n: u32, k: &[u8]| {
            nodes[(n - 1) as usize][part_of(k)]
                .borrow()
                .get(k)
                .unwrap()
                .as_deref()
                == Some(&b"v"[..])
        };

        let keys: Vec<Vec<u8>> = (0..400).map(|i| format!("k{i:04}").into_bytes()).collect();
        for (i, k) in keys.iter().enumerate() {
            let ver = Version {
                hlc: 100 + i as u64,
                node: 9,
            };
            for n in ring.replicas(k) {
                nodes[(n - 1) as usize][part_of(k)]
                    .borrow_mut()
                    .apply(k, Some(b"v"), ver)
                    .unwrap();
            }
        }
        for node in &nodes {
            for s in node {
                s.borrow_mut().sync().unwrap();
            }
        }
        assert!(keys
            .iter()
            .all(|k| ring.replicas(k).iter().all(|&n| present(&nodes, n, k))));

        let victim = 2u32;
        let vi = (victim - 1) as usize;
        let failed = 1usize;
        let lost: Vec<usize> = layout
            .partitions()
            .iter()
            .filter(|p| p.drive == failed)
            .map(|p| p.partition)
            .collect();
        assert!(
            !lost.is_empty(),
            "the failed drive must hold some partitions"
        );

        nodes[vi].clear();
        ds[vi].replace_drive(failed, DRIVE_BLOCKS);
        nodes[vi] = build_stores_with(&ds[vi], &layout, geom(), victim, |pid| lost.contains(&pid))
            .unwrap()
            .into_iter()
            .map(RefCell::new)
            .collect();

        for k in &keys {
            if ring.replicas(k).contains(&victim) {
                let survived = !lost.contains(&part_of(k));
                assert_eq!(
                    present(&nodes, victim, k),
                    survived,
                    "post-swap, node 2 holds a key iff its partition survived"
                );
            }
        }

        #[allow(clippy::needless_range_loop)]
        for s in 0..P {
            let p1 = nodes[0][s].borrow();
            let p3 = nodes[2][s].borrow();
            let peers = [(1u32, LocalPeer(&*p1)), (3u32, LocalPeer(&*p3))];
            vals_rt::block_on(
                sweep(&nodes[vi][s], victim, &ring, DEFAULT_BUCKETS, &peers),
                || {},
            )
            .unwrap();
        }

        for k in &keys {
            assert_eq!(
                present(&nodes, victim, k),
                ring.replicas(k).contains(&victim),
                "node 2 healed to exactly its owned keys"
            );
            for &n in &[1u32, 3] {
                if ring.replicas(k).contains(&n) {
                    assert!(present(&nodes, n, k), "peer {n} kept its data");
                }
            }
        }
    }

    struct FaultDriveSet<'a> {
        inner: &'a MemDriveSet,
        ops: std::cell::Cell<usize>,
        fail_at: usize,
    }
    impl<'a> FaultDriveSet<'a> {
        fn new(inner: &'a MemDriveSet, fail_at: usize) -> Self {
            FaultDriveSet {
                inner,
                ops: std::cell::Cell::new(0),
                fail_at,
            }
        }
        fn tick(&self) -> Result<(), io::Error> {
            let n = self.ops.get() + 1;
            self.ops.set(n);
            if n == self.fail_at {
                return Err(io::Error::other("injected crash"));
            }
            Ok(())
        }
    }
    impl DriveSet for FaultDriveSet<'_> {
        type Dev = MemBlockDevice;
        fn drive_count(&self) -> usize {
            self.inner.drive_count()
        }
        fn drive_blocks(&self, d: usize) -> u64 {
            self.inner.drive_blocks(d)
        }
        fn read_layout_fingerprint(&self) -> Result<Option<Vec<u8>>, io::Error> {
            self.inner.read_layout_fingerprint()
        }
        fn write_layout_fingerprint(&self, fp: &[u8]) -> Result<(), io::Error> {
            self.inner.write_layout_fingerprint(fp)
        }
        fn read_relocate_journal(&self) -> Result<Option<Vec<u8>>, io::Error> {
            self.inner.read_relocate_journal()
        }
        fn write_relocate_journal(&self, j: &[u8]) -> Result<(), io::Error> {
            self.tick()?;
            self.inner.write_relocate_journal(j)
        }
        fn clear_relocate_journal(&self) -> Result<(), io::Error> {
            self.inner.clear_relocate_journal()
        }
        fn open_region(
            &self,
            drive: usize,
            base: u64,
            len: u64,
            create: bool,
        ) -> Result<MemBlockDevice, io::Error> {
            if create {
                self.tick()?;
            }
            self.inner.open_region(drive, base, len, create)
        }
    }

    fn layout_of(parts: &[(usize, usize, u64)], region: u64, cores: usize) -> Layout {
        Layout {
            partitions: parts
                .iter()
                .map(|&(partition, drive, base)| Partition {
                    partition,
                    drive,
                    base,
                    len: region,
                    core: partition % cores,
                })
                .collect(),
            region_blocks: region,
            num_cores: cores,
        }
    }

    fn crash_sweep(mk: impl Fn() -> MemDriveSet, old: &Layout, new: &Layout) {
        let key = |p: usize| format!("k{p}");
        let mut k = 1usize;
        loop {
            let ds = mk();
            {
                let mut stores = build_stores(&ds, old, geom(), 9, true).unwrap();
                for (i, s) in stores.iter_mut().enumerate() {
                    s.apply(key(i).as_bytes(), Some(b"v"), Version { hlc: 1, node: 9 })
                        .unwrap();
                    s.sync().unwrap();
                }
            }
            let faulted = FaultDriveSet::new(&ds, k);
            let first = relocate(&faulted, old, new, geom(), 9);
            let no_more_faults = first.is_ok();
            if first.is_err() {
                let clean = FaultDriveSet::new(&ds, usize::MAX);
                relocate(&clean, old, new, geom(), 9)
                    .unwrap_or_else(|e| panic!("resume after crash at op {k} failed: {e}"));
            }
            assert!(ds.read_relocate_journal().unwrap().is_none());
            verify_layout(&ds, new, false).unwrap();
            let stores = build_stores(&ds, new, geom(), 9, false).unwrap();
            for (i, s) in stores.iter().enumerate() {
                assert_eq!(
                    s.get(key(i).as_bytes()).unwrap().as_deref(),
                    Some(&b"v"[..]),
                    "partition {i} lost data (crash at op {k})"
                );
            }
            if no_more_faults {
                break;
            }
            k += 1;
        }
    }

    #[test]
    fn relocate_survives_a_crash_at_every_point_on_drive_add() {
        let caps3 = vec![1024u64, 1024, 1024, 0];
        let caps4 = vec![1024u64; 4];
        let old = plan(&caps3, 64, 12, 8).unwrap();
        let new = replan(&old, &caps4, 8).unwrap();
        assert_ne!(old, new);
        crash_sweep(|| MemDriveSet::uniform(256, 1024, 4), &old, &new);
    }

    #[test]
    fn relocate_swaps_partitions_in_a_cycle() {
        let old = layout_of(&[(0, 0, 0), (1, 0, 64), (2, 0, 128)], 64, 4);
        let new = layout_of(&[(0, 0, 64), (1, 0, 128), (2, 0, 0)], 64, 4);
        let ds = MemDriveSet::uniform(256, 1024, 1);
        {
            let mut stores = build_stores(&ds, &old, geom(), 9, true).unwrap();
            for (i, s) in stores.iter_mut().enumerate() {
                s.apply(
                    format!("k{i}").as_bytes(),
                    Some(b"v"),
                    Version { hlc: 1, node: 9 },
                )
                .unwrap();
                s.sync().unwrap();
            }
        }
        assert_eq!(relocate(&ds, &old, &new, geom(), 9).unwrap(), 3);
        let stores = build_stores(&ds, &new, geom(), 9, false).unwrap();
        for (i, s) in stores.iter().enumerate() {
            assert_eq!(
                s.get(format!("k{i}").as_bytes()).unwrap().as_deref(),
                Some(&b"v"[..]),
                "cycle member {i} lost data"
            );
        }
    }

    #[test]
    fn relocate_swap_cycle_survives_a_crash_at_every_point() {
        let old = layout_of(&[(0, 0, 0), (1, 0, 64)], 64, 4);
        let new = layout_of(&[(0, 0, 64), (1, 0, 0)], 64, 4);
        crash_sweep(|| MemDriveSet::uniform(256, 1024, 2), &old, &new);
    }

    #[test]
    fn relocate_cycle_without_a_spare_slot_refuses_and_destroys_nothing() {
        let old = layout_of(&[(0, 0, 0), (1, 0, 64)], 64, 4);
        let new = layout_of(&[(0, 0, 64), (1, 0, 0)], 64, 4);
        let ds = MemDriveSet::with_caps(256, vec![128]);
        {
            let mut stores = build_stores(&ds, &old, geom(), 9, true).unwrap();
            for (i, s) in stores.iter_mut().enumerate() {
                s.apply(
                    format!("k{i}").as_bytes(),
                    Some(b"v"),
                    Version { hlc: 1, node: 9 },
                )
                .unwrap();
                s.sync().unwrap();
            }
        }
        match relocate(&ds, &old, &new, geom(), 9) {
            Err(LayoutError::NoSpareSlot) => {}
            other => panic!("expected NoSpareSlot, got {other:?}"),
        }
        let stores = build_stores(&ds, &old, geom(), 9, false).unwrap();
        for (i, s) in stores.iter().enumerate() {
            assert_eq!(
                s.get(format!("k{i}").as_bytes()).unwrap().as_deref(),
                Some(&b"v"[..]),
                "refused relocate must not touch source data"
            );
        }
    }

    #[test]
    fn relocate_journal_guards_reopen_and_rejects_a_mismatched_relayout() {
        let caps3 = vec![1024u64, 1024, 1024, 0];
        let caps4 = vec![1024u64; 4];
        let old = plan(&caps3, 64, 12, 8).unwrap();
        let new = replan(&old, &caps4, 8).unwrap();
        let ds = MemDriveSet::uniform(256, 1024, 4);
        {
            let mut stores = build_stores(&ds, &old, geom(), 9, true).unwrap();
            for (i, s) in stores.iter_mut().enumerate() {
                s.apply(
                    format!("k{i}").as_bytes(),
                    Some(b"v"),
                    Version { hlc: 1, node: 9 },
                )
                .unwrap();
                s.sync().unwrap();
            }
        }
        let faulted = FaultDriveSet::new(&ds, 2);
        assert!(relocate(&faulted, &old, &new, geom(), 9).is_err());
        assert!(
            ds.read_relocate_journal().unwrap().is_some(),
            "a crashed relocate leaves its journal behind"
        );
        match verify_layout(&ds, &new, false) {
            Err(LayoutError::RelocateInProgress) => {}
            other => panic!("expected RelocateInProgress, got {other:?}"),
        }
        let other_new = plan(&[2048u64, 1024, 1024, 1024], 64, 12, 8).unwrap();
        assert_ne!(other_new, new);
        match relocate(&ds, &old, &other_new, geom(), 9) {
            Err(LayoutError::Corrupt(_)) => {}
            other => panic!("expected Corrupt for a mismatched journal, got {other:?}"),
        }
        relocate(&ds, &old, &new, geom(), 9).unwrap();
        assert!(ds.read_relocate_journal().unwrap().is_none());
        verify_layout(&ds, &new, false).unwrap();
        let stores = build_stores(&ds, &new, geom(), 9, false).unwrap();
        for (i, s) in stores.iter().enumerate() {
            assert_eq!(
                s.get(format!("k{i}").as_bytes()).unwrap().as_deref(),
                Some(&b"v"[..]),
                "partition {i} lost data across the resumed relocate"
            );
        }
    }
}
