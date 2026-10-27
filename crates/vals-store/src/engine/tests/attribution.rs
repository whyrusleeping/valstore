use super::*;

struct WriteCountDevice {
    inner: FileBlockDevice,
    bytes: Arc<AtomicU64>,
}
impl BlockDevice for WriteCountDevice {
    fn block_size(&self) -> usize { self.inner.block_size() }
    fn num_blocks(&self) -> u64 { self.inner.num_blocks() }
    fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> { self.inner.read_at(lba, buf) }
    fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
        self.bytes.fetch_add(buf.len() as u64, AtomicOrdering::Relaxed);
        self.inner.write_at(lba, buf)
    }
    fn flush(&self) -> vals_spdk::Result<()> { self.inner.flush() }
}

#[test]
fn failed_tail_writes_count_physical_bytes_but_not_legacy_vlog_bytes() {
    if isolated_env_test(&[]) { return; }
    let armed = Arc::new(AtomicBool::new(false));
    let mut e = Engine::create_prewiped(TailWriteFailDevice {
        inner: MemBlockDevice::new(BS, 4096), armed: armed.clone(),
    }, Config { seg_blocks: 8, reserved_blocks: 0, ordering: Ordering::Sorted });
    e.put(b"k", &[7; 80], 1).unwrap();
    let before = crate::attribution::writes();
    let legacy = crate::vlog::vlog_write_bytes();
    armed.store(true, AtomicOrdering::Relaxed);
    assert!(e.sync().is_err());
    let failed = crate::attribution::writes();
    assert_eq!(failed[0].1, before[0].1);
    assert_eq!(failed[1].1 - before[1].1, BS as u64);
    assert_eq!(crate::vlog::vlog_write_bytes(), legacy);
    armed.store(false, AtomicOrdering::Relaxed);
    e.sync().unwrap();
    let success = crate::attribution::writes();
    assert_eq!(success[0].1, before[0].1);
    assert_eq!(success[1].1 - before[1].1, 2 * BS as u64);
    assert_eq!(crate::vlog::vlog_write_bytes() - legacy, BS as u64);
}

#[test]
fn physical_write_sources_account_for_aligned_file_device_bytes() {
    if isolated_env_test(&[("VALS_VLOG_CO_COMPACT", Some("1")),
        ("VALS_MERGE_TIER_K", Some("64")), ("VALS_MERGE_TIER_MIN_K", Some("2")),
        ("VALS_MAINT_PACE", Some("0"))]) { return; }
    let path = temp_path("attribution");
    let bytes = Arc::new(AtomicU64::new(0));
    let dev = WriteCountDevice { inner: FileBlockDevice::create(&path, 512, 8192).unwrap(), bytes: bytes.clone() };
    let before: Vec<_> = crate::attribution::writes().into_iter().map(|(_, n)| n).collect();
    let vlog_before = crate::vlog::vlog_write_bytes();
    let mut e = Engine::create_prewiped(dev, Config { seg_blocks: 8, reserved_blocks: 0, ordering: Ordering::Sorted });
    for version in 1..=2 {
        for i in 0..100 {
            e.put(format!("k{i:05}").as_bytes(), &[7; 80], version).unwrap();
        }
        let plan = e.flush_plan(RunBuildOpts::default()).unwrap().unwrap();
        let written = vals_rt::block_on(build_and_write_flush(plan, e.device()), || {}).unwrap();
        e.flush_commit(written).unwrap();
    }
    let plan = e.compact_plan(RunBuildOpts::default(), None).unwrap().unwrap();
    let written = vals_rt::block_on(compact_and_write(plan, e.dev.clone()), || {}).unwrap();
    e.compact_commit(written).unwrap();
    let deltas: Vec<_> = crate::attribution::writes().iter().zip(before).map(|((_, n), b)| n - b).collect();
    assert_eq!(deltas.iter().sum::<u64>(), bytes.load(AtomicOrdering::Relaxed));
    for i in 0..6 { assert!(deltas[i] > 0, "source {i} absent: {deltas:?}"); }
    assert_eq!(deltas[6], 0);
    assert_eq!(crate::vlog::vlog_write_bytes() - vlog_before, deltas[0] + deltas[1]);
    drop(e);
    std::fs::remove_file(path).unwrap();
}
