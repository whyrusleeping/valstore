use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine};

use super::*;
use crate::partition::RangeMap;
use crate::pd::{AddressedMembers, PdDaemon, PdStateView, RingConfig};
use crate::replica::Stored;
use crate::wire::RtFragment;
use crate::ring::{NodeId, Ring};
use crate::zone::Catalog;

fn v(hlc: u64, node: u64) -> Version {
    Version { hlc, node }
}

fn put(key: &str, version: Version, value: &str) -> (Vec<u8>, Stored) {
    (key.as_bytes().to_vec(), Stored { version, value: Some(value.as_bytes().to_vec()) })
}

fn tomb(key: &str, version: Version) -> (Vec<u8>, Stored) {
    (key.as_bytes().to_vec(), Stored { version, value: None })
}

struct VecSource {
    label: String,
    pages: VecDeque<ReplicaPage>,
    rts: Vec<RtFragment>,
}

impl ReplicaSource for VecSource {
    fn label(&self) -> String {
        self.label.clone()
    }

    fn next_page(&mut self) -> Result<Option<ReplicaPage>, String> {
        Ok(self.pages.pop_front())
    }

    fn range_tombstones(&mut self) -> Result<Vec<RtFragment>, String> {
        Ok(self.rts.clone())
    }
}

fn source(label: &str, pages: Vec<ReplicaPage>) -> Box<dyn ReplicaSource> {
    Box::new(VecSource { label: label.into(), pages: pages.into(), rts: Vec::new() })
}

fn rt(lo: Option<&str>, hi: Option<&str>, version: Version) -> RtFragment {
    RtFragment { lo: lo.map(|b| b.as_bytes().to_vec()), hi: hi.map(|b| b.as_bytes().to_vec()), version }
}

fn merged(sources: Vec<Box<dyn ReplicaSource>>) -> Result<Vec<Record>, String> {
    let mut writer = RecordWriter::new(Vec::new(), 7).unwrap();
    merge_replicas(sources, None, &mut writer)?;
    let (bytes, _) = writer.finish().unwrap();
    let mut reader = RecordReader::new(bytes.as_slice()).unwrap();
    let mut out = Vec::new();
    while let Some(record) = reader.next_record().unwrap() {
        out.push(record);
    }
    Ok(out)
}

fn record(key: &str, version: Version, value: Option<&str>) -> Record {
    Record { key: key.as_bytes().to_vec(), version, value: value.map(|v| v.as_bytes().to_vec()) }
}

#[test]
fn merge_keeps_one_lww_winner_per_key_across_replicas_and_pages() {
    let a = source("a", vec![
        vec![put("a", v(5, 1), "a5"), put("b", v(5, 1), "b5")],
        vec![put("d", v(3, 1), "d3")],
    ]);
    let b = source("b", vec![
        vec![put("a", v(5, 1), "a5"), tomb("b", v(9, 2))],
        vec![],
        vec![put("c", v(1, 2), "c1"), put("d", v(4, 2), "d4")],
    ]);
    let c = source("c", vec![vec![put("a", v(4, 3), "stale"), put("e", v(2, 3), "only-c")]]);
    let empty = source("empty", vec![]);
    assert_eq!(merged(vec![a, b, c, empty]).unwrap(), vec![
        record("a", v(5, 1), Some("a5")),
        record("b", v(9, 2), None),
        record("c", v(1, 2), Some("c1")),
        record("d", v(4, 2), Some("d4")),
        record("e", v(2, 3), Some("only-c")),
    ]);
}

#[test]
fn merge_refuses_replicas_that_disagree_at_one_version_or_return_keys_out_of_order() {
    let value = merged(vec![
        source("a", vec![vec![put("k", v(5, 1), "x")]]),
        source("b", vec![vec![put("k", v(5, 1), "y")]]),
    ])
    .unwrap_err();
    assert!(value.contains("identical version") && value.contains("values"), "{value}");
    let kind = merged(vec![
        source("a", vec![vec![put("k", v(5, 1), "x")]]),
        source("b", vec![vec![tomb("k", v(5, 1))]]),
    ])
    .unwrap_err();
    assert!(kind.contains("put vs tombstone"), "{kind}");
    for older in [Some("y"), None] {
        let newest = || source("newest", vec![vec![put("k", v(9, 1), "z")]]);
        let x = || source("x", vec![vec![put("k", v(5, 1), "x")]]);
        let y = || source("y", vec![vec![match older {
            Some(value) => put("k", v(5, 1), value),
            None => tomb("k", v(5, 1)),
        }]]);
        for order in [vec![newest(), x(), y()], vec![x(), newest(), y()], vec![x(), y(), newest()]] {
            let error = merged(order).unwrap_err();
            assert!(error.contains("identical version"), "a losing-version conflict was accepted: {error}");
        }
    }
    let order = merged(vec![source("a", vec![vec![put("b", v(1, 1), "b")], vec![put("a", v(1, 1), "a")]])])
        .unwrap_err();
    assert!(order.contains("out of order"), "{order}");
}

#[test]
fn record_files_round_trip_and_reject_disorder_and_tampering() {
    let mut writer = RecordWriter::new(Vec::new(), 3).unwrap();
    writer.push(&record("a", v(1, 2), Some(""))).unwrap();
    writer.push(&record("b", v(3, 4), None)).unwrap();
    assert!(writer.push(&record("b", v(5, 6), Some("x"))).is_err());
    let (bytes, summary) = writer.finish().unwrap();
    assert_eq!(summary.counts, RecordCounts { records: 2, puts: 1, tombstones: 1 });
    assert_eq!(summary.bytes, bytes.len() as u64);
    assert_eq!(summary.crc32c, vals_store::crc::crc32(&bytes));
    let mut reader = RecordReader::new(bytes.as_slice()).unwrap();
    assert_eq!(reader.zone_id(), 3);
    assert_eq!(reader.next_record().unwrap(), Some(record("a", v(1, 2), Some(""))));
    assert_eq!(reader.next_record().unwrap(), Some(record("b", v(3, 4), None)));
    assert_eq!(reader.next_record().unwrap(), None);
    assert_eq!(reader.file_summary(), Some((summary.bytes, summary.crc32c)));

    let read_all = |bytes: &[u8]| -> Result<(), String> {
        let mut reader = RecordReader::new(bytes)?;
        while reader.next_record()?.is_some() {}
        Ok(())
    };
    for cut in [bytes.len() - 1, bytes.len() - 5, 12, 11] {
        assert!(read_all(&bytes[..cut]).is_err(), "truncation at {cut} accepted");
    }
    let mut flipped = bytes.clone();
    flipped[13] ^= 1;
    assert!(read_all(&flipped).is_err());
    let mut extra = bytes.clone();
    extra.push(0);
    assert!(read_all(&extra).unwrap_err().contains("after the trailer"));
}

fn manifest() -> Manifest {
    Manifest {
        backup_id: 0xabc,
        created_unix_ms: 17,
        tool: "valsd 0.0.1 test".into(),
        source_incarnation: 0x1234,
        pd_applied: 9,
        pd_applied_end: 11,
        ring_vnodes: 64,
        ring_rf: 3,
        pd_state: FileEntry { name: PD_STATE_FILE.into(), bytes: 100, crc32c: 0xdead_beef },
        zones: vec![ZoneEntry {
            id: 0,
            name: "default".into(),
            ordering: Ordering::Unsorted,
            rf: 3,
            file: FileEntry { name: zone_file_name(0), bytes: 5, crc32c: 1 },
            counts: RecordCounts { records: 3, puts: 2, tombstones: 1 },
        }],
    }
}

#[test]
fn manifest_round_trips_and_parses_strictly() {
    let m = manifest();
    let text = m.render();
    assert!(text.starts_with("vals-backup 1\n"));
    assert_eq!(Manifest::parse(&text).unwrap(), m);
    let bad = [
        text.replace("vals-backup 1", "vals-backup 2"),
        text.replace("end\n", ""),
        format!("{text}extra\n"),
        text.replace("ring 64 3", "ring 64 3\nmystery 1"),
        text.replace("pd_applied 9", "pd_applied 9\npd_applied 9"),
        text.replace(" 3 2 1\n", " 3 2 2\n"),
        text.replace("zone-0.vrec", "../zone-0.vrec"),
    ];
    for text in bad {
        assert!(Manifest::parse(&text).is_err(), "accepted:\n{text}");
    }
}

#[test]
fn placement_change_allows_token_preserving_splits_only() {
    let base = PdStateView {
        incarnation: Some(1),
        lineage: None,
        members: vec![(1, 1, Vec::new())],
        zones: Catalog::new(),
        range_maps: HashMap::from([(5, RangeMap::single())]),
    };
    let mut split = base.clone();
    let mut map = RangeMap::single();
    assert!(map.split(b"m"));
    split.range_maps.insert(5, map.clone());
    assert_eq!(placement_change(&base, &split), None, "an inherit split moves no data");
    let mut moved = split.clone();
    moved.range_maps.insert(
        5,
        RangeMap::from_splits_tokens_and_pending(
            vec![b"m".to_vec()],
            vec![Vec::new(), b"m".to_vec()],
            vec![None, None],
        )
        .unwrap(),
    );
    assert!(placement_change(&split, &moved).unwrap().contains("range placement of zone 5"));
    let mut pending = split.clone();
    let mut map = map.clone();
    assert!(map.set_pending(1, b"m".to_vec()));
    pending.range_maps.insert(5, map);
    assert!(placement_change(&split, &pending).is_some(), "a new pending re-home target is a change");
    let mut members = base.clone();
    members.members.push((2, 1, Vec::new()));
    assert!(placement_change(&base, &members).unwrap().contains("membership"));
    let mut incarnation = base.clone();
    incarnation.incarnation = Some(2);
    assert!(placement_change(&base, &incarnation).is_some());
}

fn engine() -> Engine<MemBlockDevice> {
    Engine::create_prewiped(MemBlockDevice::new(4096, 4096), Config::default())
}

fn wait_capture(pd: &PdDaemon, done: impl Fn(&PdStateCapture) -> bool) -> PdStateCapture {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(capture) = pd.capture_state().and_then(|view| capture_from_inspection(view).ok()) {
            if done(&capture) {
                return capture;
            }
        }
        assert!(Instant::now() < deadline, "PD state never reached the expected shape");
        std::thread::sleep(Duration::from_millis(10));
    }
}

struct FakeCluster {
    captures: VecDeque<PdStateCapture>,
    data: BTreeMap<ZoneId, Vec<ReplicaPage>>,
    rts: BTreeMap<ZoneId, Vec<Vec<RtFragment>>>,
}

impl BackupCluster for FakeCluster {
    fn capture(&mut self) -> Result<PdStateCapture, String> {
        self.captures.pop_front().ok_or_else(|| "no capture left".to_string())
    }

    fn ring(&mut self, _members: &AddressedMembers) -> Result<(u32, usize), String> {
        Ok((64, 1))
    }

    fn sources(
        &mut self,
        _members: &AddressedMembers,
        zone_id: ZoneId,
        _cfg: &ZoneConfig,
    ) -> Result<Vec<Box<dyn ReplicaSource>>, String> {
        let rts = self.rts.get(&zone_id).cloned().unwrap_or_default();
        Ok(self
            .data
            .get(&zone_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .enumerate()
            .map(|(i, entries)| -> Box<dyn ReplicaSource> {
                Box::new(VecSource {
                    label: format!("replica {i}"),
                    pages: VecDeque::from([entries]),
                    rts: rts.get(i).cloned().unwrap_or_default(),
                })
            })
            .collect())
    }
}

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("vals-backup-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn create_writes_a_verifiable_backup_and_verify_catches_damage() {
    let pd = PdDaemon::spawn_single(1, engine(), RingConfig { vnodes: 64, rf: 1 }, Vec::new()).unwrap();
    pd.add_node(1).unwrap();
    pd.create_zone(ZoneConfig { name: "s".into(), rf: 1, ordering: Ordering::Sorted, scope: None }).unwrap();
    let capture = wait_capture(&pd, |c| {
        let view = c.view();
        view.incarnation.is_some() && view.zones.by_name("s").is_some()
    });
    let zone = capture.view().zones.by_name("s").unwrap();
    pd.add_node(2).unwrap();
    let grown = wait_capture(&pd, |c| c.view().members.len() == 2);
    pd.shutdown();

    let data = BTreeMap::from([
        (0, vec![vec![put("x", v(1, 1), "x1")], vec![put("x", v(2, 1), "x2"), tomb("y", v(3, 1))]]),
        (zone, vec![vec![put("a", v(1, 1), "a"), put("b", v(1, 1), "b")]]),
    ]);
    let root = scratch("create");
    let out = root.join("bk");
    let mut cluster = FakeCluster { captures: VecDeque::from([capture.clone(), capture.clone()]), data: data.clone(), rts: BTreeMap::new() };
    let manifest = create_backup(&out, &mut cluster, "test", StaleCopies::Fail, &mut |_| {}).unwrap();
    assert_eq!(manifest.source_incarnation, capture.view().incarnation.unwrap());
    assert_eq!(
        manifest.zones.iter().map(|z| (z.id, z.counts.records)).collect::<Vec<_>>(),
        vec![(0, 2), (zone, 2)]
    );
    let (_, report) = verify(&out).unwrap();
    assert_eq!(report.counts, RecordCounts { records: 4, puts: 3, tombstones: 1 });
    assert!(create_backup(&out, &mut cluster, "test", StaleCopies::Fail, &mut |_| {}).unwrap_err().contains("exists"));

    let changed = root.join("changed");
    let mut cluster = FakeCluster { captures: VecDeque::from([capture.clone(), grown]), data, rts: BTreeMap::new() };
    let error = create_backup(&changed, &mut cluster, "test", StaleCopies::Fail, &mut |_| {}).unwrap_err();
    assert!(error.contains("membership changed"), "{error}");
    assert!(!changed.exists(), "a discarded backup leaves no directory behind");

    let damaged = |name: &str, damage: &dyn Fn(&Path)| -> String {
        let copy = root.join(name);
        std::fs::create_dir(&copy).unwrap();
        for entry in std::fs::read_dir(&out).unwrap() {
            let entry = entry.unwrap();
            std::fs::copy(entry.path(), copy.join(entry.file_name())).unwrap();
        }
        damage(&copy);
        verify(&copy).unwrap_err()
    };
    let zone_name = zone_file_name(zone);
    let flip = damaged("flip", &|dir| {
        let path = dir.join(&zone_name);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[14] ^= 0x10;
        std::fs::write(&path, bytes).unwrap();
    });
    assert!(!flip.is_empty());
    let cut = damaged("truncate", &|dir| {
        let path = dir.join(&zone_name);
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..bytes.len() - 3]).unwrap();
    });
    assert!(cut.contains("truncated"), "{cut}");
    let missing = damaged("missing", &|dir| std::fs::remove_file(dir.join(&zone_name)).unwrap());
    assert!(missing.contains(&zone_name), "{missing}");
    let count = damaged("count", &|dir| {
        let path = dir.join(MANIFEST_FILE);
        let text = std::fs::read_to_string(&path).unwrap();
        let line = text.lines().find(|l| l.starts_with(&format!("zone {zone} "))).unwrap();
        let fixed = line.replace(" 2 2 0", " 3 3 0");
        std::fs::write(&path, text.replace(line, &fixed)).unwrap();
    });
    assert!(count.contains("MANIFEST records"), "{count}");
    let pd_state = damaged("pd-state", &|dir| {
        let path = dir.join(PD_STATE_FILE);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[20] ^= 1;
        std::fs::write(&path, bytes).unwrap();
    });
    assert!(pd_state.contains("CRC-32C"), "{pd_state}");
    let no_manifest = damaged("incomplete", &|dir| std::fs::remove_file(dir.join(MANIFEST_FILE)).unwrap());
    assert!(no_manifest.contains("incomplete"), "{no_manifest}");
    let _ = std::fs::remove_dir_all(&root);
}

struct SmallBackup {
    root: PathBuf,
    out: PathBuf,
    capture: PdStateCapture,
    zone: ZoneId,
    data: BTreeMap<ZoneId, Vec<ReplicaPage>>,
}

fn small_backup(name: &str) -> SmallBackup {
    let pd = PdDaemon::spawn_single(1, engine(), RingConfig { vnodes: 64, rf: 1 }, Vec::new()).unwrap();
    pd.add_node(1).unwrap();
    pd.create_zone(ZoneConfig { name: "s".into(), rf: 1, ordering: Ordering::Sorted, scope: None }).unwrap();
    let capture = wait_capture(&pd, |c| {
        let view = c.view();
        view.incarnation.is_some() && view.zones.by_name("s").is_some()
    });
    pd.shutdown();
    let zone = capture.view().zones.by_name("s").unwrap();
    let data = BTreeMap::from([
        (0, vec![vec![put("x", v(1, 1), "x1")], vec![put("x", v(2, 1), "x2"), tomb("y", v(3, 1))]]),
        (zone, vec![vec![put("a", v(1, 1), "a"), put("b", v(1, 1), "b")]]),
    ]);
    let root = scratch(name);
    let out = root.join("bk");
    let mut cluster = FakeCluster { captures: VecDeque::from([capture.clone(), capture.clone()]), data: data.clone(), rts: BTreeMap::new() };
    create_backup(&out, &mut cluster, "test", StaleCopies::Fail, &mut |_| {}).unwrap();
    SmallBackup { root, out, capture, zone, data }
}

fn target_members() -> AddressedMembers {
    vec![(1, 1, vec!["127.0.0.1:9001".parse().unwrap()])]
}

#[test]
fn restore_pd_seeds_a_pd_that_boots_with_a_new_incarnation_and_lineage() {
    let bk = small_backup("restore-pd");
    let manifest = open_backup(&bk.out).unwrap().manifest;
    let target = |name: &str| RestoreTarget {
        raft_dir: bk.root.join(name),
        voters: vec![1],
        members: target_members(),
        ring: (64, 1),
        boot_zones: vec![ZoneConfig { name: "s".into(), rf: 1, ordering: Ordering::Sorted, scope: None }],
        opaque_range_maps: false,
    };
    let refuse = |change: &dyn Fn(&mut RestoreTarget)| -> String {
        let mut t = target("refused");
        change(&mut t);
        let error = restore_pd(&bk.out, &t, 42).unwrap_err();
        assert!(!t.raft_dir.exists(), "a refused restore writes nothing");
        error
    };
    assert!(refuse(&|t| t.ring = (32, 1)).contains("vnodes 32"));
    assert!(refuse(&|t| t.boot_zones[0].name = "other".into()).contains("\"other\""));
    assert!(refuse(&|t| t.boot_zones[0].rf = 2).contains("rf 2"));
    assert!(refuse(&|t| t.members[0].1 = 3).contains("weight"));
    assert!(refuse(&|t| t.members.push((2, 1, vec!["127.0.0.1:9002".parse().unwrap()]))).contains("weight"));
    let source = manifest.source_incarnation;
    assert!(restore_pd(&bk.out, &target("same"), source).unwrap_err().contains("new"));

    let t = target("raft");
    let restored = restore_pd(&bk.out, &t, 42).unwrap();
    assert_eq!(restored.lineage, lineage_of(&manifest));
    assert_eq!((restored.index, restored.term), (bk.capture.applied, bk.capture.term));
    assert!(restore_pd(&bk.out, &t, 42).unwrap_err().contains("not empty"));
    let twin = target("twin");
    assert_eq!(restore_pd(&bk.out, &twin, 42).unwrap().digest, restored.digest, "same inputs, same seed");
    let other = target("other-incarnation");
    assert_ne!(restore_pd(&bk.out, &other, 43).unwrap().digest, restored.digest);
    let mut moved = target("other-address");
    moved.members[0].2 = vec!["127.0.0.1:9002".parse().unwrap()];
    assert_ne!(restore_pd(&bk.out, &moved, 42).unwrap().digest, restored.digest);

    let pd = PdDaemon::spawn_single_durable_bounded(1, 1 << 20, RingConfig { vnodes: 64, rf: 1 }, Vec::new(), &t.raft_dir)
        .unwrap();
    let booted = wait_capture(&pd, |c| c.applied > restored.index);
    pd.shutdown();
    let view = booted.view();
    assert_eq!(view.incarnation, Some(42));
    assert_eq!(view.lineage, Some(lineage_of(&manifest)));
    assert_eq!(view.members, target_members());
    assert_eq!(view.zones, bk.capture.view().zones);
    let backup = open_backup(&bk.out).unwrap();
    check_restored(&backup, &view).unwrap();
    assert!(check_restored(&backup, &bk.capture.view()).unwrap_err().contains("lineage"));
    let _ = std::fs::remove_dir_all(&bk.root);
}

#[test]
fn compare_matches_exact_copies_tolerates_collected_tombstones_and_counts_differences() {
    let bk = small_backup("compare");
    let backup = open_backup(&bk.out).unwrap();
    let entries =
        crate::pd::restored_state(&backup.pd_state.entries, &target_members(), 42, lineage_of(&backup.manifest))
            .unwrap();
    let restored = PdStateCapture { applied: 50, term: 2, captured_unix_ms: 0, entries };
    let compare = |data: BTreeMap<ZoneId, Vec<ReplicaPage>>| {
        let mut cluster = FakeCluster { captures: VecDeque::from([restored.clone(), restored.clone()]), data, rts: BTreeMap::new() };
        compare_backup(&bk.out, &mut cluster, &mut |_| {})
    };

    let exact = compare(bk.data.clone()).unwrap();
    assert_eq!(exact.differences(), 0, "{:?}", exact.samples);
    assert_eq!(exact.zones.iter().map(|z| z.matched).sum::<u64>(), 4);

    let collected = compare(BTreeMap::from([
        (0, vec![vec![put("x", v(2, 1), "x2")]]),
        (bk.zone, bk.data[&bk.zone].clone()),
    ]))
    .unwrap();
    assert_eq!(collected.differences(), 0);
    assert_eq!(collected.zones[0].collected_tombstones, 1);

    let diverged = compare(BTreeMap::from([
        (0, vec![vec![put("x", v(2, 1), "other"), tomb("y", v(3, 1)), put("z", v(1, 1), "z")]]),
        (bk.zone, vec![vec![put("b", v(1, 1), "b")]]),
    ]))
    .unwrap();
    assert_eq!(diverged.differences(), 3, "{:?}", diverged.samples);
    assert_eq!(diverged.samples.len(), 3);
    assert!(diverged.samples.iter().any(|s| s.contains("the target has no such key")), "{:?}", diverged.samples);
    assert!(diverged.samples.iter().any(|s| s.contains("the backup has no such key")), "{:?}", diverged.samples);

    let mut moved = restored.clone();
    moved.entries.retain(|(key, _)| key.len() != 4);
    let mut cluster = FakeCluster { captures: VecDeque::from([restored.clone(), moved]), data: bk.data.clone(), rts: BTreeMap::new() };
    assert!(compare_backup(&bk.out, &mut cluster, &mut |_| {}).unwrap_err().contains("membership"));
    let mut cluster = FakeCluster { captures: VecDeque::from([bk.capture.clone()]), data: bk.data.clone(), rts: BTreeMap::new() };
    assert!(compare_backup(&bk.out, &mut cluster, &mut |_| {}).unwrap_err().contains("lineage"));
    let _ = std::fs::remove_dir_all(&bk.root);
}

#[test]
fn load_batches_are_bounded_by_records_and_bytes() {
    let mut bytes = Vec::new();
    let mut writer = RecordWriter::new(&mut bytes, 0).unwrap();
    let sizes = [10usize, 10, 10, 200, 10, 10, 10, 10, 10];
    for (i, size) in sizes.iter().enumerate() {
        let value = (i != 6).then(|| vec![b'v'; *size]);
        writer.push(&Record { key: format!("k{i}").into_bytes(), version: v(1, 1), value }).unwrap();
    }
    writer.finish().unwrap();
    let mut reader = RecordReader::new(bytes.as_slice()).unwrap();
    let mut carry = None;
    let mut batches = Vec::new();
    loop {
        let batch = super::restore::next_batch(&mut reader, &mut carry, 3, 140).unwrap();
        if batch.is_empty() {
            break;
        }
        batches.push(batch.iter().map(|r| String::from_utf8(r.key.clone()).unwrap()).collect::<Vec<_>>().join(","));
    }
    assert_eq!(batches, vec!["k0,k1,k2", "k3", "k4,k5,k6", "k7,k8"]);
    assert!(carry.is_none());
    assert_eq!(reader.counts().records, sizes.len() as u64);
}

fn encoded_rt(lo: Option<&str>, hi: Option<&str>, hlc: u64) -> Vec<u8> {
    let mut b = vec![TAG_RANGE_TOMBSTONE];
    for bound in [lo, hi] {
        match bound {
            Some(bound) => {
                b.push(1);
                b.extend_from_slice(&(bound.len() as u32).to_le_bytes());
                b.extend_from_slice(bound.as_bytes());
            }
            None => b.push(0),
        }
    }
    b.extend_from_slice(&hlc.to_le_bytes());
    b.extend_from_slice(&1u64.to_le_bytes());
    b
}

fn encoded_put(key: &str, hlc: u64, value: &str) -> Vec<u8> {
    let mut b = vec![TAG_PUT];
    b.extend_from_slice(&(key.len() as u32).to_le_bytes());
    b.extend_from_slice(key.as_bytes());
    b.extend_from_slice(&hlc.to_le_bytes());
    b.extend_from_slice(&1u64.to_le_bytes());
    b.extend_from_slice(&(value.len() as u32).to_le_bytes());
    b.extend_from_slice(value.as_bytes());
    b
}

fn record_file(version: u16, body: &[Vec<u8>], count: u64) -> Vec<u8> {
    let mut b = RECORDS_MAGIC.to_le_bytes().to_vec();
    b.extend_from_slice(&version.to_le_bytes());
    b.extend_from_slice(&4u32.to_le_bytes());
    b.extend(body.concat());
    b.push(TAG_END);
    b.extend_from_slice(&count.to_le_bytes());
    let crc = vals_store::crc::crc32(&b);
    b.extend_from_slice(&crc.to_le_bytes());
    b
}

fn read_file(bytes: &[u8]) -> Result<(Vec<Record>, Vec<RtFragment>), String> {
    let mut reader = RecordReader::new(bytes)?;
    let mut records = Vec::new();
    while let Some(record) = reader.next_record()? {
        records.push(record);
    }
    Ok((records, reader.range_tombstones().to_vec()))
}

#[test]
fn record_files_carry_range_tombstones_after_points_and_version_1_files_still_load() {
    let mut writer = RecordWriter::new(Vec::new(), 4).unwrap();
    writer.push(&record("a", v(1, 2), Some("x"))).unwrap();
    writer.push_range_tombstone(&rt(None, Some("b"), v(5, 1))).unwrap();
    writer.push_range_tombstone(&rt(Some("c"), Some("d"), v(6, 1))).unwrap();
    assert!(writer.push_range_tombstone(&rt(Some("c5"), Some("e"), v(7, 1))).is_err(), "overlap accepted");
    assert!(writer.push_range_tombstone(&rt(Some("f"), Some("f"), v(7, 1))).is_err(), "empty span accepted");
    assert!(writer.push(&record("z", v(1, 1), None)).is_err(), "a point record after a range tombstone accepted");
    writer.push_range_tombstone(&rt(Some("d"), None, v(7, 1))).unwrap();
    assert!(writer.push_range_tombstone(&rt(Some("x"), None, v(8, 1))).is_err(), "a range after an unbounded one");
    let (bytes, summary) = writer.finish().unwrap();
    assert_eq!((summary.counts, summary.range_tombstones), (RecordCounts { records: 1, puts: 1, tombstones: 0 }, 3));
    assert_eq!(bytes[4..6], RECORDS_VERSION.to_le_bytes());
    assert_eq!(u64::from_le_bytes(bytes[bytes.len() - 12..bytes.len() - 4].try_into().unwrap()), 4);
    let (records, rts) = read_file(&bytes).unwrap();
    assert_eq!(records, vec![record("a", v(1, 2), Some("x"))]);
    assert_eq!(rts, vec![rt(None, Some("b"), v(5, 1)), rt(Some("c"), Some("d"), v(6, 1)), rt(Some("d"), None, v(7, 1))]);

    let v1 = record_file(1, &[encoded_put("k", 3, "v")], 1);
    assert_eq!(read_file(&v1).unwrap(), (vec![record("k", v(3, 1), Some("v"))], Vec::new()));
    let v1_range = record_file(1, &[encoded_put("k", 3, "v"), encoded_rt(Some("a"), Some("b"), 4)], 2);
    assert!(read_file(&v1_range).unwrap_err().contains("bad record tag 3"));
    let ok = record_file(2, &[encoded_put("k", 3, "v"), encoded_rt(Some("a"), Some("b"), 4)], 2);
    assert_eq!(read_file(&ok).unwrap().1, vec![rt(Some("a"), Some("b"), v(4, 1))]);
    let overlap = record_file(2, &[encoded_rt(Some("a"), Some("c"), 4), encoded_rt(Some("b"), Some("d"), 4)], 2);
    assert!(read_file(&overlap).unwrap_err().contains("order"));
    let point_after = record_file(2, &[encoded_rt(Some("a"), Some("b"), 4), encoded_put("k", 3, "v")], 2);
    assert!(read_file(&point_after).unwrap_err().contains("follows a range tombstone"));
    let miscounted = record_file(2, &[encoded_put("k", 3, "v"), encoded_rt(Some("a"), Some("b"), 4)], 1);
    assert!(read_file(&miscounted).unwrap_err().contains("trailer counts"));
}

#[test]
fn backups_merge_every_replicas_range_tombstones_omit_covered_records_and_compare_them() {
    let pd = PdDaemon::spawn_single(1, engine(), RingConfig { vnodes: 64, rf: 1 }, Vec::new()).unwrap();
    pd.add_node(1).unwrap();
    pd.create_zone(ZoneConfig { name: "s".into(), rf: 1, ordering: Ordering::Sorted, scope: None }).unwrap();
    let capture = wait_capture(&pd, |c| {
        let view = c.view();
        view.incarnation.is_some() && view.zones.by_name("s").is_some()
    });
    pd.shutdown();
    let zone = capture.view().zones.by_name("s").unwrap();
    let data = BTreeMap::from([(zone, vec![
        vec![put("a", v(9, 1), "a9"), put("b", v(3, 1), "b3"), put("c", v(8, 1), "c8"), tomb("d", v(4, 1))],
        vec![put("a", v(9, 1), "a9"), tomb("b", v(5, 1)), put("c", v(8, 1), "c8"), put("x", v(2, 1), "x2")],
    ])]);
    let rts = BTreeMap::from([(zone, vec![
        vec![rt(Some("w"), Some("x"), v(6, 1))],
        vec![rt(Some("b"), Some("e"), v(5, 1)), rt(Some("x"), Some("y"), v(6, 1))],
    ])]);
    let root = scratch("ranges");
    let out = root.join("bk");
    let mut cluster =
        FakeCluster { captures: VecDeque::from([capture.clone(), capture.clone()]), data: data.clone(), rts: rts.clone() };
    create_backup(&out, &mut cluster, "test", StaleCopies::Fail, &mut |_| {}).unwrap();
    let (_, report) = verify(&out).unwrap();
    assert_eq!((report.counts.records, report.range_tombstones), (2, 2));
    let (records, held) = read_file(&std::fs::read(out.join(zone_file_name(zone))).unwrap()).unwrap();
    assert_eq!(records, vec![record("a", v(9, 1), Some("a9")), record("c", v(8, 1), Some("c8"))]);
    assert_eq!(held, vec![rt(Some("b"), Some("e"), v(5, 1)), rt(Some("w"), Some("y"), v(6, 1))]);

    let backup = open_backup(&out).unwrap();
    let entries =
        crate::pd::restored_state(&backup.pd_state.entries, &target_members(), 42, lineage_of(&backup.manifest))
            .unwrap();
    let restored = PdStateCapture { applied: 50, term: 2, captured_unix_ms: 0, entries };
    let compare = |rts: BTreeMap<ZoneId, Vec<Vec<RtFragment>>>| {
        let mut cluster =
            FakeCluster { captures: VecDeque::from([restored.clone(), restored.clone()]), data: data.clone(), rts };
        compare_backup(&out, &mut cluster, &mut |_| {}).unwrap()
    };
    let exact = compare(rts.clone());
    assert_eq!(exact.differences(), 0, "{:?}", exact.samples);
    let zone_cmp = exact.zones.iter().find(|z| z.id == zone).unwrap();
    assert_eq!((zone_cmp.matched, zone_cmp.matched_range_tombstones), (2, 2));
    let missing = compare(BTreeMap::from([(zone, vec![vec![rt(Some("b"), Some("e"), v(5, 1))]; 2])]));
    assert_eq!(missing.differences(), 2, "{:?}", missing.samples);
    assert!(missing.samples.iter().any(|s| s.contains("the backup holds it, the target does not")), "{:?}", missing.samples);
    assert!(missing.samples.iter().any(|s| s.contains("the backup has no such key")), "{:?}", missing.samples);
    let _ = std::fs::remove_dir_all(&root);
}

struct HeldSource {
    holder: (NodeId, usize),
    pages: VecDeque<ReplicaPage>,
    floor_ms: u64,
    floor_calls: std::rc::Rc<std::cell::Cell<usize>>,
}

impl ReplicaSource for HeldSource {
    fn label(&self) -> String {
        format!("node {} shard {}", self.holder.0, self.holder.1)
    }

    fn next_page(&mut self) -> Result<Option<ReplicaPage>, String> {
        Ok(self.pages.pop_front())
    }

    fn holder(&self) -> Option<(NodeId, usize)> {
        Some(self.holder)
    }

    fn gc_floor_ms(&mut self) -> Result<u64, String> {
        self.floor_calls.set(self.floor_calls.get() + 1);
        Ok(self.floor_ms)
    }
}

fn at_ms(ms: u64, node: u64) -> Version {
    v(ms << crate::hlc::COUNTER_BITS, node)
}

fn three_nodes(zones: Catalog, range_maps: HashMap<ZoneId, RangeMap>) -> PdStateView {
    let addr = |port: u16| vec![std::net::SocketAddr::from(([127, 0, 0, 1], port))];
    PdStateView {
        incarnation: Some(1),
        lineage: None,
        members: vec![(1, 1, addr(9001)), (2, 1, addr(9002)), (3, 1, addr(9003))],
        zones,
        range_maps,
    }
}

fn key_where(prefix: &str, want: impl Fn(&[(NodeId, usize)]) -> bool, placement: &ZonePlacement) -> String {
    (0..10_000)
        .map(|i| format!("{prefix}-{i}"))
        .find(|key| want(&placement.owners(key.as_bytes())))
        .expect("some key has the wanted placement")
}

fn fenced_merge(
    placement: &ZonePlacement,
    data: Vec<(NodeId, u64, Vec<(Vec<u8>, Stored)>)>,
) -> (Vec<Record>, MergeStats, usize) {
    let calls = std::rc::Rc::new(std::cell::Cell::new(0));
    let sources = data
        .into_iter()
        .map(|(node, floor_ms, mut entries)| -> Box<dyn ReplicaSource> {
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            Box::new(HeldSource {
                holder: (node, 0),
                pages: VecDeque::from([entries]),
                floor_ms,
                floor_calls: calls.clone(),
            })
        })
        .collect();
    let mut out = Vec::new();
    let stats = merge_replicas_into(sources, Some(placement), &mut |record| {
        out.push(record);
        Ok(())
    })
    .unwrap();
    (out, stats, calls.get())
}

#[test]
fn merge_omits_a_former_owner_put_below_every_owner_gc_floor_and_keeps_everything_else() {
    let view = three_nodes(Catalog::new(), HashMap::new());
    let placement = ZonePlacement::new(&view, (64, 2), DEFAULT_ZONE, &ZoneConfig::default_config(2));
    let stray = |prefix: &str| key_where(prefix, |owners| !owners.iter().any(|o| o.0 == 3), &placement);
    let (zombie, fresh, deleted, older, shared) =
        (stray("zombie"), stray("fresh"), stray("deleted"), stray("older"), stray("shared"));
    let owned_by_3 = key_where("own3", |owners| owners.iter().any(|o| o.0 == 3), &placement);
    let floor = 10_000;
    let entry = |key: &str, ms: u64, value: Option<&str>| match value {
        Some(value) => put(key, at_ms(ms, 1), value),
        None => tomb(key, at_ms(ms, 1)),
    };
    let owners_of = |key: &str| placement.owners(key.as_bytes()).into_iter().map(|o| o.0).collect::<Vec<_>>();
    let mut per_node: BTreeMap<NodeId, Vec<(Vec<u8>, Stored)>> = BTreeMap::new();
    for node in owners_of(&older) {
        per_node.entry(node).or_default().push(entry(&older, 1_000, Some("owner-old")));
    }
    for node in owners_of(&shared) {
        per_node.entry(node).or_default().push(entry(&shared, 5_000, Some("both")));
    }
    per_node.entry(3).or_default().extend([
        entry(&zombie, 5_000, Some("zombie")),
        entry(&fresh, 20_000, Some("fresh")),
        entry(&deleted, 5_000, None),
        entry(&older, 6_000, Some("stale-newer")),
        entry(&shared, 5_000, Some("both")),
        entry(&owned_by_3, 2_000, Some("old-but-owned")),
    ]);
    let data = per_node.into_iter().map(|(node, entries)| (node, floor, entries)).collect();
    let older_key = older.clone();
    let (records, stats, floor_calls) = fenced_merge(&placement, data);
    let got: BTreeMap<String, (u64, Option<String>)> = records
        .into_iter()
        .map(|r| {
            (String::from_utf8(r.key).unwrap(), (r.version.physical_ms(), r.value.map(|v| String::from_utf8(v).unwrap())))
        })
        .collect();
    let want: BTreeMap<String, (u64, Option<String>)> = [
        (fresh, (20_000, Some("fresh".to_string()))),
        (deleted, (5_000, None)),
        (older, (1_000, Some("owner-old".to_string()))),
        (shared, (5_000, Some("both".to_string()))),
        (owned_by_3, (2_000, Some("old-but-owned".to_string()))),
    ]
    .into_iter()
    .collect();
    assert_eq!(got, want, "only the stale former-owner puts are left out");
    assert!(!got.contains_key(&zombie));
    assert_eq!(stats.stale_copies_omitted, 2, "the zombie and the stale newer copy of `older`");
    assert!(floor_calls > 0);

    let (records, stats, _) = fenced_merge(&placement, vec![
        (1, floor, Vec::new()),
        (2, 4_000, Vec::new()),
        (3, floor, vec![entry(&zombie, 5_000, Some("zombie"))]),
    ]);
    assert!(records.is_empty(), "one owner whose floor is above the copy may have collected its delete");
    assert_eq!(stats.stale_copies_omitted, 1);
    assert!(stats.stale_samples[0].contains("held only by node 3"), "{:?}", stats.stale_samples);

    let (records, stats, _) = fenced_merge(&placement, vec![
        (1, 4_000, Vec::new()),
        (2, 5_000, Vec::new()),
        (3, floor, vec![entry(&zombie, 5_000, Some("zombie"))]),
    ]);
    assert_eq!(records.len(), 1, "every owner would still admit the copy, so none collected a newer delete");
    assert_eq!(stats.stale_copies_omitted, 0);

    let owner = owners_of(&older_key)[0];
    let (records, _, floor_calls) = fenced_merge(&placement, vec![
        (owner, floor, vec![entry(&older_key, 1_000, Some("owner-old"))]),
        (3, floor, Vec::new()),
    ]);
    assert_eq!(records.len(), 1);
    assert_eq!(floor_calls, 0, "no floor is fetched unless a non-owner put would win");

    let mut unfenced = Vec::new();
    merge_replicas_into(
        vec![
            Box::new(HeldSource {
                holder: (3, 0),
                pages: VecDeque::from([vec![entry(&zombie, 5_000, Some("zombie"))]]),
                floor_ms: floor,
                floor_calls: Default::default(),
            }),
        ],
        None,
        &mut |record| {
            unfenced.push(record);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(unfenced.len(), 1, "without placement nothing is fenced");
}

#[test]
fn merge_trusts_only_current_owners_and_fences_pending_or_aborted_rehome_targets_leftovers() {
    let zone = 7;
    let mut zones = Catalog::new();
    zones.insert(zone, ZoneConfig { name: "s".into(), rf: 1, ordering: Ordering::Sorted, scope: None });
    let ring = Ring::from_weighted(1, 64, [(1, 1), (2, 1), (3, 1)]);
    let owner = |token: &[u8]| ring.replicas_in_zone(zone, token, 1, None)[0];
    let split = (0..1000)
        .map(|i| format!("m{i:03}").into_bytes())
        .find(|split| owner(split) != owner(b""))
        .expect("some token moves the range");
    let (source, target) = (owner(b""), owner(&split));
    let other = [1, 2, 3].into_iter().find(|n| *n != source && *n != target).unwrap();
    let mut key = split.clone();
    key.extend_from_slice(b"/k");
    let map = |pending: Option<Vec<u8>>| {
        RangeMap::from_splits_tokens_and_pending(vec![split.clone()], vec![Vec::new(), Vec::new()], vec![None, pending])
            .unwrap()
    };
    let key_str = String::from_utf8(key.clone()).unwrap();
    let merge = |placement: &ZonePlacement, on_source: Option<u64>, on_target: Option<u64>, target_floor: u64| {
        let copy = |ms: Option<u64>| ms.map(|ms| vec![put(&key_str, at_ms(ms, 1), "copy")]).unwrap_or_default();
        let (records, stats, _) = fenced_merge(placement, vec![
            (source, 10_000, copy(on_source)),
            (target, target_floor, copy(on_target)),
            (other, 10_000, Vec::new()),
        ]);
        (records.len(), stats.stale_copies_omitted)
    };
    let pending = three_nodes(zones.clone(), HashMap::from([(zone, map(Some(split.clone())))]));
    let pending = ZonePlacement::new(&pending, (64, 1), zone, &zones.get(zone).unwrap().clone());
    assert!(pending.owners(&key).iter().any(|o| o.0 == target));
    assert_eq!(merge(&pending, Some(5_000), Some(5_000), 0), (1, 0), "a copy the current owner also holds is kept");
    assert_eq!(merge(&pending, None, Some(20_000), 0), (1, 0), "a young pending-target write is kept");
    assert_eq!(
        merge(&pending, None, Some(5_000), 0),
        (0, 1),
        "an old copy only the pending target holds is a former owner's leftover the source may have collected"
    );
    let (records, stats, _) = fenced_merge(&pending, vec![
        (source, 19_000, Vec::new()),
        (target, 0, Vec::new()),
        (other, 10_000, vec![put(&key_str, at_ms(5_000, 1), "zombie")]),
    ]);
    assert!(
        records.is_empty() && stats.stale_copies_omitted == 1,
        "an empty pending target's low floor does not vouch for a copy the source may have collected"
    );
    let aborted = three_nodes(zones.clone(), HashMap::from([(zone, map(None))]));
    let aborted = ZonePlacement::new(&aborted, (64, 1), zone, &zones.get(zone).unwrap().clone());
    assert_eq!(merge(&aborted, None, Some(5_000), 10_000), (0, 1), "after an abort the target's old leftover is stale");
    let (records, _, _) = fenced_merge(&aborted, vec![
        (source, 10_000, Vec::new()),
        (target, 10_000, vec![put(&key_str, at_ms(20_000, 1), "young")]),
        (other, 10_000, Vec::new()),
    ]);
    assert_eq!(records.len(), 1, "a leftover the owner would still admit is kept");
}
