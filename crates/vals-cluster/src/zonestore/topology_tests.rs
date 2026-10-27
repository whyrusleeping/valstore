use super::*;
use crate::drive::{self, DriveSet};
use crate::hlc::Version;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

const PARTITION_BLOCKS: u64 = 2048;

struct CountDev {
    inner: MemBlockDevice,
    writes: AtomicUsize,
}

impl BlockDevice for CountDev {
    fn block_size(&self) -> usize { self.inner.block_size() }
    fn num_blocks(&self) -> u64 { self.inner.num_blocks() }
    fn read_at(&self, lba: u64, buf: &mut [u8]) -> BlockResult<()> {
        self.inner.read_at(lba, buf)
    }
    fn write_at(&self, lba: u64, buf: &[u8]) -> BlockResult<()> {
        self.writes.fetch_add(1, AtomicOrdering::Relaxed);
        self.inner.write_at(lba, buf)
    }
    fn flush(&self) -> BlockResult<()> {
        self.writes.fetch_add(1, AtomicOrdering::Relaxed);
        self.inner.flush()
    }
    fn deallocate(&self, ranges: &[(u64, u64)]) -> BlockResult<bool> {
        self.writes.fetch_add(1, AtomicOrdering::Relaxed);
        self.inner.deallocate(ranges)
    }
}

struct Namespace {
    dev: Arc<CountDev>,
}

impl DriveSet for Namespace {
    type Dev = SubDev<CountDev>;
    fn drive_count(&self) -> usize { 1 }
    fn drive_blocks(&self, _: usize) -> u64 { self.dev.num_blocks() }
    fn open_region(&self, drive: usize, base: u64, len: u64, _: bool) -> std::io::Result<Self::Dev> {
        assert_eq!(drive, 0);
        Ok(SubDev::new(self.dev.clone(), base, len))
    }
}

fn cfg() -> RegionZoneConfig {
    RegionZoneConfig {
        super_blocks: 8, zone_region_blocks: 512, zone_region_overrides: Vec::new(),
        seg_blocks: 4, reserved_blocks: 0,
    }
}

impl Namespace {
    fn new() -> Self {
        Self { dev: Arc::new(CountDev {
            inner: MemBlockDevice::new(256, 8 * PARTITION_BLOCKS), writes: AtomicUsize::new(0),
        }) }
    }

    fn multi(&self, count: usize, create: bool) -> Result<Vec<RegionZoneStore<SubDev<CountDev>>>, drive::LayoutError> {
        let layout = drive::plan(&drive::drive_caps(self), PARTITION_BLOCKS, count, count)?;
        open_partition_zone_stores(self, &layout, &cfg(), 7, create)
    }

    fn single(&self, node_id: u32, config: &RegionZoneConfig, create: bool) -> Result<RegionZoneStore<CountDev>, ZoneStoreError> {
        let expected = ZoneTopology::single(node_id, self.dev.num_blocks(), config, self.dev.block_size());
        RegionZoneStore::open_checked(self.dev.clone(), config, node_id, create, expected)
    }

    fn snapshot(&self) -> (Vec<u8>, usize) {
        let mut bytes = vec![0; self.dev.block_size() * self.dev.num_blocks() as usize];
        self.dev.read_at(0, &mut bytes).unwrap();
        (bytes, self.dev.writes.load(AtomicOrdering::Relaxed))
    }
}

fn refused<T, E: fmt::Display>(result: Result<T, E>) {
    let err = result.err().expect("wrong topology must refuse").to_string();
    assert!(err.contains("run `valsd provision` while the node is offline"), "{err}");
    assert!(err.contains("topology/geometry mismatch") || err.contains("legacy zone layout")
        || err.contains("layout identities differ"), "expected topology refusal, got: {err}");
}

#[test]
fn single_over_multi_refuses_without_writes() {
    let ns = Namespace::new();
    ns.multi(8, true).unwrap();
    let before = ns.snapshot();
    refused(ns.single(7, &cfg(), false));
    assert_eq!(ns.snapshot(), before);
}

#[test]
fn multi_over_single_refuses_without_writes() {
    let ns = Namespace::new();
    ns.single(7, &cfg(), true).unwrap();
    let before = ns.snapshot();
    refused(ns.multi(8, false));
    assert_eq!(ns.snapshot(), before);
}

#[test]
fn multi_over_single_with_stale_partition_headers_refuses_without_writes() {
    let ns = Namespace::new();
    let providers = ns.multi(8, true).unwrap();
    for provider in providers {
        provider.open(0, Ordering::Unsorted).unwrap();
    }
    let old = ns.snapshot().0;
    let single = ns.single(7, &cfg(), true).unwrap();
    single.open(0, Ordering::Unsorted).unwrap();
    let before = ns.snapshot();
    let offset = PARTITION_BLOCKS as usize * ns.dev.block_size();
    assert_eq!(&old[offset..], &before.0[offset..], "partitions 1..7 remain intact and stale");
    refused(ns.multi(8, false));
    assert_eq!(ns.snapshot(), before);
}

#[test]
fn eight_to_four_partitions_refuses_without_writes() {
    let ns = Namespace::new();
    ns.multi(8, true).unwrap();
    let before = ns.snapshot();
    refused(ns.multi(4, false));
    assert_eq!(ns.snapshot(), before);
}

#[test]
fn node_id_mismatch_refuses_without_writes() {
    let ns = Namespace::new();
    ns.single(7, &cfg(), true).unwrap();
    let before = ns.snapshot();
    refused(ns.single(8, &cfg(), false));
    assert_eq!(ns.snapshot(), before);
}

#[test]
fn geometry_mismatch_refuses_without_writes() {
    let ns = Namespace::new();
    ns.single(7, &cfg(), true).unwrap();
    let before = ns.snapshot();
    refused(ns.single(7, &RegionZoneConfig { seg_blocks: 8, ..cfg() }, false));
    assert_eq!(ns.snapshot(), before);
}

#[test]
fn legacy_v2_refuses_without_writes() {
    let ns = Namespace::new();
    let mut rec = encode_superblock(0, &[], None, None, None);
    rec[4..6].copy_from_slice(&2u16.to_le_bytes());
    rec.remove(SUPER_HEADER_BYTES);
    let end = rec.len() - 4;
    let crc = crc32(&rec[..end]);
    rec[end..].copy_from_slice(&crc.to_le_bytes());
    assert_eq!(decode_superblock(&rec).unwrap().unwrap().3, None);
    let mut block = vec![0; ns.dev.block_size()];
    block[..rec.len()].copy_from_slice(&rec);
    ns.dev.write_at(0, &block).unwrap();
    let before = ns.snapshot();
    refused(ns.single(7, &cfg(), false));
    assert_eq!(ns.snapshot(), before);
}

#[test]
fn mixed_layout_ids_refuse_before_any_engine_opens() {
    let ns = Namespace::new();
    ns.multi(8, true).unwrap();
    let layout = drive::plan(&drive::drive_caps(&ns), PARTITION_BLOCKS, 8, 8).unwrap();
    let p = layout.partitions().last().unwrap();
    let parent = Arc::new(ns.open_region(p.drive, p.base, p.len, true).unwrap());
    let expected = ZoneTopology::partition(123, 7, 8, p, &cfg(), parent.block_size());
    RegionZoneStore::open_checked(parent, &cfg(), 7, true, expected).unwrap();
    let before = ns.snapshot();
    refused(ns.multi(8, false));
    assert_eq!(ns.snapshot(), before);
}

fn write_data<D: BlockDevice>(provider: &RegionZoneStore<D>) {
    let mut store = provider.open(0, Ordering::Unsorted).unwrap();
    store.apply(b"kept", Some(b"value"), Version { hlc: 5, node: 7 }).unwrap();
    store.sync().unwrap();
    provider.liveness_log().store(1234).unwrap();
    provider.open(2, Ordering::Sorted).unwrap();
    provider.drop_zone(2).unwrap();
}

fn read_data<D: BlockDevice>(provider: &RegionZoneStore<D>) {
    assert_eq!(provider.liveness_log().load(), Some(1234));
    let store = provider.open_existing(0).unwrap();
    assert_eq!(store.get(b"kept").unwrap().as_deref(), Some(&b"value"[..]));
}

fn seed_variable_zones<D: BlockDevice>(provider: &RegionZoneStore<D>) -> Vec<ZoneRegion> {
    for zone in [0, 1] {
        let mut store = provider.open(zone, Ordering::Unsorted).unwrap();
        store.apply(b"kept", Some(b"value"), Version { hlc: 5, node: 7 }).unwrap();
        store.sync().unwrap();
    }
    provider.state.lock().unwrap().table.regions()
}

fn check_variable_zones<D: BlockDevice>(provider: &RegionZoneStore<D>, original: &[ZoneRegion]) {
    assert_eq!(provider.state.lock().unwrap().table.regions(), original);
    provider.open(2, Ordering::Unsorted).unwrap();
    provider.open(3, Ordering::Unsorted).unwrap();
    {
        let state = provider.state.lock().unwrap();
        assert_eq!(state.table.get(2).unwrap().len, 1024);
        assert_eq!(state.table.get(3).unwrap().len, 256);
        for region in original {
            assert_eq!(state.table.get(region.zone_id), Some(*region));
        }
    }
    for zone in [0, 1] {
        let store = provider.open_existing(zone).unwrap();
        assert_eq!(store.get(b"kept").unwrap().as_deref(), Some(&b"value"[..]));
    }
}

#[test]
fn checked_reopen_allocation_size_changes_preserve_existing_zones() {
    let initial = RegionZoneConfig { zone_region_overrides: vec![(1, 128)], ..cfg() };
    let changed = RegionZoneConfig {
        zone_region_blocks: 1024,
        zone_region_overrides: vec![(0, 128), (1, 256), (3, 256)],
        ..initial.clone()
    };
    let ns = Namespace::new();
    let layout = drive::plan(&drive::drive_caps(&ns), PARTITION_BLOCKS, 8, 8).unwrap();
    let originals: Vec<_> = open_partition_zone_stores(&ns, &layout, &initial, 7, true)
        .unwrap().iter().map(seed_variable_zones).collect();
    let before = ns.snapshot();
    let reopened = open_partition_zone_stores(&ns, &layout, &changed, 7, false).unwrap();
    assert_eq!(ns.snapshot(), before);
    for (provider, original) in reopened.iter().zip(&originals) {
        check_variable_zones(provider, original);
    }

    let ns = Namespace::new();
    let original = seed_variable_zones(&ns.single(7, &initial, true).unwrap());
    let before = ns.snapshot();
    let reopened = ns.single(7, &changed, false).unwrap();
    assert_eq!(ns.snapshot(), before);
    check_variable_zones(&reopened, &original);
}

#[test]
fn matching_topologies_reopen_and_preserve_data_and_identity() {
    let ns = Namespace::new();
    let providers = ns.multi(8, true).unwrap();
    let layout_id = providers[0].layout_id();
    for provider in providers {
        assert_eq!(provider.layout_id(), layout_id);
        write_data(&provider);
    }
    for provider in ns.multi(8, false).unwrap() {
        assert_eq!(provider.layout_id(), layout_id);
        read_data(&provider);
    }
    let ns = Namespace::new();
    let single = ns.single(7, &cfg(), true).unwrap();
    let layout_id = single.layout_id();
    write_data(&single);
    let reopened = ns.single(7, &cfg(), false).unwrap();
    assert_eq!(reopened.layout_id(), layout_id);
    read_data(&reopened);
}
