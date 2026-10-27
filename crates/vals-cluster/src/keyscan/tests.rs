use super::*;
use crate::drive::{MemDriveSet, PartitionGeom};
use crate::node::Node;
use crate::zone::Ordering;
use crate::zonestore::{RegionZoneStore, ZoneStore};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use vals_spdk::{MemBlockDevice, WriteHandle};
use vals_store::run::RunBuildOpts;

struct TrapDev {
    inner: MemBlockDevice,
    writes: Arc<AtomicUsize>,
}

impl TrapDev {
    fn reject(&self) -> vals_spdk::Result<()> {
        self.writes.fetch_add(1, AtomicOrdering::Relaxed);
        Err(io::Error::other("keyscan attempted mutation").into())
    }
}

impl BlockDevice for TrapDev {
    fn block_size(&self) -> usize { self.inner.block_size() }
    fn num_blocks(&self) -> u64 { self.inner.num_blocks() }
    fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> { self.inner.read_at(lba, buf) }
    fn write_at(&self, _: u64, _: &[u8]) -> vals_spdk::Result<()> { self.reject() }
    fn submit_write(&self, _: u64, _: &[u8]) -> WriteHandle { WriteHandle::ready(self.reject()) }
    fn flush(&self) -> vals_spdk::Result<()> { self.reject() }
    fn deallocate(&self, _: &[(u64, u64)]) -> vals_spdk::Result<bool> { self.reject().map(|_| false) }
}

struct TrapDs {
    inner: MemDriveSet,
    writes: Arc<AtomicUsize>,
    fail_region: Option<(usize, u64)>,
}

impl DriveSet for TrapDs {
    type Dev = TrapDev;
    fn drive_count(&self) -> usize { self.inner.drive_count() }
    fn drive_blocks(&self, d: usize) -> u64 { self.inner.drive_blocks(d) }
    fn read_layout_fingerprint(&self) -> io::Result<Option<Vec<u8>>> { self.inner.read_layout_fingerprint() }
    fn read_relocate_journal(&self) -> io::Result<Option<Vec<u8>>> { self.inner.read_relocate_journal() }
    fn write_layout_fingerprint(&self, _: &[u8]) -> io::Result<()> {
        self.writes.fetch_add(1, AtomicOrdering::Relaxed);
        Err(io::Error::other("fingerprint write"))
    }
    fn write_relocate_journal(&self, _: &[u8]) -> io::Result<()> {
        self.writes.fetch_add(1, AtomicOrdering::Relaxed);
        Err(io::Error::other("journal write"))
    }
    fn clear_relocate_journal(&self) -> io::Result<()> {
        self.writes.fetch_add(1, AtomicOrdering::Relaxed);
        Err(io::Error::other("journal clear"))
    }
    fn open_region(&self, d: usize, base: u64, len: u64, create: bool) -> io::Result<TrapDev> {
        assert!(!create);
        if self.fail_region == Some((d, base)) {
            return Err(io::Error::other("injected partition open failure"));
        }
        Ok(TrapDev { inner: self.inner.open_region(d, base, len, false)?, writes: self.writes.clone() })
    }
}

fn config() -> KeyscanConfig {
    KeyscanConfig {
        node_id: 1, partitions: 2, region_blocks: 16384,
        zones: RegionZoneConfig { super_blocks: 16, zone_region_blocks: 1024,
            zone_region_overrides: vec![(2, 8192)], seg_blocks: 16, reserved_blocks: 0 },
        zone_id: 2, partition: None,
    }
}

fn temp(name: &str) -> std::path::PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    std::env::temp_dir().join(format!("vals-keyscan-{name}-{}-{}", std::process::id(), NEXT.fetch_add(1, AtomicOrdering::Relaxed)))
}

fn snapshots(ds: &MemDriveSet, cfg: &KeyscanConfig) -> Vec<Vec<u8>> {
    let layout = drive::plan(&drive::drive_caps(ds), cfg.region_blocks, cfg.partitions, cfg.partitions).unwrap();
    layout.partitions().iter().map(|p| {
        let dev = ds.open_region(p.drive, p.base, p.len, false).unwrap();
        let mut bytes = vec![0; dev.capacity_bytes() as usize];
        dev.read_at(0, &mut bytes).unwrap();
        bytes
    }).collect()
}

fn ids(path: &Path) -> HashSet<CensusId> {
    let bytes = std::fs::read(path).unwrap();
    assert_eq!(bytes.len() % 12, 0);
    bytes.chunks_exact(12).map(|b| b.try_into().unwrap()).collect()
}

fn rpc(addr: std::net::SocketAddr, req: &crate::wire::Request) -> crate::wire::Response {
    use std::io::Read;
    let mut stream = std::net::TcpStream::connect(addr).unwrap();
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
    crate::proto::client_handshake_blocking(&mut stream).unwrap();
    let payload = crate::wire::encode_request_payload(req);
    stream.write_all(&((8 + payload.len()) as u32).to_le_bytes()).unwrap();
    stream.write_all(&1u64.to_le_bytes()).unwrap();
    stream.write_all(&payload).unwrap();
    let mut len = [0; 4];
    stream.read_exact(&mut len).unwrap();
    let mut buf = vec![0; u32::from_le_bytes(len) as usize];
    stream.read_exact(&mut buf).unwrap();
    crate::wire::decode_response(&buf[8..]).unwrap()
}

#[test]
fn keyscan_ptr_crc_error_and_json_escaping() {
    let dev = MemBlockDevice::new(256, 4096);
    let shared = dev.share();
    let mut engine = Engine::create(dev, vals_store::Config {
        seg_blocks: 16, reserved_blocks: 0, ordering: vals_store::Ordering::Sorted,
    }).unwrap();
    let key = b"key\"\\\n\0\xff";
    engine.put(key, b"value", 7).unwrap();
    engine.flush(RunBuildOpts::default()).unwrap().unwrap();
    let (page, _) = engine.scan_winners_page(None, None, 1, usize::MAX).unwrap();
    let entry = page[0].1;
    let text = details(&engine, 0, key, &entry).unwrap();
    assert!(text.contains("\"key_matches\":true"));
    assert!(text.contains("\\\"\\\\\\u000a\\u0000"));
    let lba = entry.ptr.segment.0 * 16 + entry.ptr.offset as u64 / 256;
    let mut bytes = vec![0; 256];
    shared.read_at(lba, &mut bytes).unwrap();
    bytes[entry.ptr.offset as usize % 256 + vals_store::vlog::HEADER_LEN] ^= 1;
    shared.write_at(lba, &bytes).unwrap();
    let text = details(&engine, 0, key, &entry).unwrap();
    assert!(text.contains("crc mismatch"));
    assert!(text.contains("\"winner\""));
}

#[test]
fn keyscan_readonly_multidrive_winners_and_versions() {
    let cfg = config();
    let ds = MemDriveSet::uniform(256, cfg.region_blocks, 2);
    let geom = PartitionGeom { seg_blocks: cfg.zones.seg_blocks, reserved_blocks: 0, ordering: Ordering::Unsorted };
    Node::provision_multidrive_zoned(cfg.node_id, &ds, cfg.partitions, cfg.region_blocks,
        cfg.zones.super_blocks, cfg.zones.zone_region_blocks, cfg.zones.zone_region_overrides.clone(), geom).unwrap();
    let layout = drive::plan(&drive::drive_caps(&ds), cfg.region_blocks, cfg.partitions, cfg.partitions).unwrap();
    let mut expected: HashSet<_> = (0..4200).map(|i| census_id(format!("bulk{i:04}").as_bytes())).collect();
    for key in [b"overwritten".as_slice(), b"mem", b"run", b"prefix", b"prefix\0"] { expected.insert(census_id(key)); }
    for p in layout.partitions() {
        let provider = RegionZoneStore::open(Arc::new(ds.open_region(p.drive, p.base, p.len, false).unwrap()), &cfg.zones, 1, false).unwrap();
        let mut store = ZoneStore::open(&provider, 2, Ordering::Sorted).unwrap();
        let engine = store.engine_mut();
        engine.put(b"overwritten", b"one", 1).unwrap();
        engine.put(b"deleted", b"old", 1).unwrap();
        engine.put(b"run", b"value", 1).unwrap();
        for i in 0..4200 { engine.put(format!("bulk{i:04}").as_bytes(), b"v", 1).unwrap(); }
        engine.flush(RunBuildOpts::default()).unwrap().unwrap();
        engine.put(b"overwritten", b"two", 2).unwrap();
        engine.put_kind(b"deleted", b"", 2, true).unwrap();
        engine.put(b"prefix", b"a", 1).unwrap();
        engine.put(b"prefix\0", b"b", 1).unwrap();
        engine.flush(RunBuildOpts::default()).unwrap().unwrap();
        engine.put(b"overwritten", b"three", 3).unwrap();
        engine.put(b"mem", b"value", 1).unwrap();
        engine.sync().unwrap();
    }
    let listeners = Node::bind_n(2).unwrap();
    let addrs: Vec<_> = listeners.iter().map(|(_, a)| *a).collect();
    let (_, handle) = Node::serve_multidrive_zoned_with_overrides(1, listeners.into_iter().map(|(l, _)| l).collect(),
        ds.share(), cfg.region_blocks, cfg.zones.super_blocks, cfg.zones.zone_region_blocks,
        cfg.zones.zone_region_overrides.clone(), geom, crate::Ring::from_members(1, 64, [1]),
        vec![(1, addrs.clone())], std::time::Duration::from_secs(3600), false).unwrap();
    let node = crate::testutil::running(handle);
    let zones = [crate::wire::WireZone { id: 2, rf: 1, ordering: crate::zone::ordering_to_u8(Ordering::Sorted), scope: None, name: "keys" }];
    for &addr in &addrs {
        rpc(addr, &crate::wire::Request::SetZones { auth: &[], epoch: 1, zones: &zones, incarnation: None });
        match rpc(addr, &crate::wire::Request::Scan { epoch: crate::wire::INSPECT_EPOCH, zone_id: 2, lo: None, hi: None, max: 10 }) {
            crate::wire::Response::Scan { entries, .. } => assert!(!entries.is_empty()),
            other => panic!("{other:?}"),
        }
    }
    node.shutdown();
    let before = snapshots(&ds, &cfg);
    let writes = Arc::new(AtomicUsize::new(0));
    let trap = TrapDs { inner: ds.share(), writes: writes.clone(), fail_region: None };
    let out = temp("ids");
    keyscan(&trap, &cfg, &out, None).unwrap();
    for p in 0..2 {
        assert_eq!(ids(&out.join(format!("p{p}.ids"))), expected);
        assert_eq!(ids(&out.join(format!("p{p}.tomb.ids"))), HashSet::from([census_id(b"deleted")]));
    }
    let summary = std::fs::read_to_string(out.join("summary.json")).unwrap();
    assert_eq!(summary.matches("\"runs\":2").count(), 2);
    assert!(!summary.contains("\"error\":\""));
    let original = std::fs::read(out.join("p0.ids")).unwrap();
    assert!(keyscan(&trap, &cfg, &out, None).is_err());
    assert_eq!(std::fs::read(out.join("p0.ids")).unwrap(), original);
    let detail = temp("details");
    keyscan(&trap, &cfg, &detail, Some(&HashSet::from([census_id(b"overwritten")]))).unwrap();
    for p in 0..2 {
        let text = std::fs::read_to_string(detail.join(format!("p{p}.jsonl"))).unwrap();
        assert_eq!(text.lines().count(), 1);
        assert_eq!(text.matches("\"key_matches\":true").count(), 3);
        assert_eq!(text.matches("\"source\":\"run\"").count(), 2);
        assert!(text.contains("\"source\":\"memtable\""));
        for version in 1..=3 { assert!(text.contains(&format!("\"version\":\"{version:032x}\""))); }
        assert!(text.contains("\"winner\":{\"version\":\"00000000000000000000000000000003\""));
    }
    let mut absent = config();
    absent.zone_id = 99;
    let missing = temp("missing");
    assert!(keyscan(&trap, &absent, &missing, None).is_err());
    assert_eq!(std::fs::read_to_string(missing.join("summary.json")).unwrap().matches("requested zone is absent").count(), 2);
    let failure = temp("partial");
    let p0 = layout.partitions()[0];
    let failed_ds = TrapDs { inner: ds.share(), writes: writes.clone(), fail_region: Some((p0.drive, p0.base)) };
    assert!(keyscan(&failed_ds, &cfg, &failure, None).is_err());
    assert!(!failure.exists(), "all partition headers are checked before creating outputs");
    assert_eq!(writes.load(AtomicOrdering::Relaxed), 0);
    assert_eq!(snapshots(&ds, &cfg), before);
    for dir in [out, detail, missing] { std::fs::remove_dir_all(dir).unwrap(); }
}
