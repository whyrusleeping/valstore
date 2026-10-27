use super::*;

fn config() -> Config {
    Config { seg_blocks: 8, reserved_blocks: 0, ordering: Ordering::Sorted }
}

fn populate<D: BlockDevice>(e: &mut Engine<D>) {
    for version in 1..=2 {
        for i in 0..40 {
            e.put(format!("k{i:03}").as_bytes(), &[version as u8; 80], version).unwrap();
        }
        e.flush(RunBuildOpts::default()).unwrap().unwrap();
    }
}

fn check_keys<D: BlockDevice>(e: &Engine<D>) {
    for i in 0..40 {
        assert_eq!(e.get(format!("k{i:03}").as_bytes()).unwrap(), Some(vec![2; 80]));
    }
}

#[test]
fn freed_segments_wait_for_successful_manifest_and_recover_after_failure() {
    let mem = MemBlockDevice::new(BS, 4096);
    let disk = mem.share();
    let mut e = Engine::create_prewiped(ManifestWriteFailDevice {
        inner: mem, armed: AtomicBool::new(false),
    }, config());
    populate(&mut e);
    e.set_device_dealloc(true);
    let stats = e.compact_runs(RunBuildOpts::default(), None).unwrap();
    assert!(stats.segments_freed > 0);
    assert_eq!(e.dealloc_step(256).segments, 0);
    e.put(b"extra", b"tail", 3).unwrap();
    e.sync().unwrap();
    e.device().armed.store(true, AtomicOrdering::Relaxed);
    assert!(e.flush(RunBuildOpts::default()).is_err());
    assert_eq!(e.dealloc_step(256).segments, 0);
    check_keys(&e);
    drop(e);
    let reopened = Engine::open(disk, config()).unwrap();
    check_keys(&reopened);
    assert_eq!(reopened.get(b"extra").unwrap().as_deref(), Some(&b"tail"[..]));
}

#[test]
fn free_before_barrier_does_not_destroy_durable_references() {
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 4096), config());
    populate(&mut e);
    e.set_device_dealloc(true);
    let seg = e.runs.last().unwrap().meta.value_segs.as_ref().unwrap()[0];
    e.alloc.free(seg).unwrap();
    assert_eq!(e.dealloc_step(256).segments, 0);
    let disk = e.device().share();
    drop(e);
    let reopened = Engine::open(disk, config()).unwrap();
    check_keys(&reopened);
}

#[test]
fn durable_merge_frees_are_zeroed_bounded_and_reopen_cleanly() {
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 4096), config());
    populate(&mut e);
    e.set_device_dealloc(true);
    let free_before: Vec<_> = e.run_handles().into_iter().flat_map(|h| h.segs).collect();
    e.compact_runs(RunBuildOpts::default(), None).unwrap();
    assert_eq!(e.dealloc_step(256).segments, 0);
    e.put(b"extra", b"tail", 3).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    let before = vals_spdk::device_dealloc_totals();
    let first = e.dealloc_step(1);
    assert_eq!(first.segments, 1);
    assert_eq!(first.bytes, 8 * BS as u64);
    let rest = e.dealloc_step(256);
    assert!(rest.segments > 0);
    let after = vals_spdk::device_dealloc_totals();
    assert!(after.commands > before.commands);
    assert!(after.bytes >= before.bytes + first.bytes + rest.bytes);
    let mut zeroed = 0;
    let mut buf = vec![0; 8 * BS];
    for seg in free_before {
        if !e.alloc.is_free(seg) { continue; }
        e.device().read_at(e.alloc.segment_range(seg).0, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0));
        zeroed += 1;
    }
    assert!(zeroed > 0);
    check_keys(&e);
    let disk = e.device().share();
    drop(e);
    let reopened = Engine::open(disk, config()).unwrap();
    check_keys(&reopened);
    assert_eq!(reopened.get(b"extra").unwrap().as_deref(), Some(&b"tail"[..]));
}

#[test]
fn co_compact_value_segments_wait_for_manifest_and_recover_after_dealloc() {
    if bottommost_co_compact_env() { return; }
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 4096), config());
    populate(&mut e);
    e.set_device_dealloc(true);
    let old_values: std::collections::HashSet<_> = e.runs.iter()
        .flat_map(|r| r.meta.value_segs.as_ref().unwrap().iter().copied()).collect();
    let plan = e.compact_plan(RunBuildOpts::default(), None).unwrap().unwrap();
    assert!(plan.co_compacts());
    let dev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, dev), || {}).unwrap();
    e.compact_commit(written).unwrap();
    assert_eq!(e.dealloc_step(256).segments, 0);
    let freed: Vec<_> = old_values.into_iter().filter(|&seg| e.alloc.is_free(seg)).collect();
    assert!(!freed.is_empty());
    e.put(b"extra", b"tail", 3).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    assert!(e.dealloc_step(256).segments > 0);
    let mut checked = 0;
    let mut buf = vec![0; 8 * BS];
    for seg in freed {
        if !e.alloc.is_free(seg) { continue; }
        e.device().read_at(e.alloc.segment_range(seg).0, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0));
        checked += 1;
    }
    assert!(checked > 0);
    check_keys(&e);
    let disk = e.device().share();
    drop(e);
    let reopened = Engine::open(disk, config()).unwrap();
    check_keys(&reopened);
    assert_eq!(reopened.get(b"extra").unwrap().as_deref(), Some(&b"tail"[..]));
}

#[test]
fn reused_segment_is_not_deallocated() {
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 4096), config());
    populate(&mut e);
    e.set_device_dealloc(true);
    let old = e.run_handles()[0].segs[0];
    e.compact_runs(RunBuildOpts::default(), None).unwrap();
    assert!(e.alloc.is_free(old));
    e.alloc.mark_allocated(old).unwrap();
    let replacement = vec![0x5a; 8 * BS];
    let lba = e.alloc.segment_range(old).0;
    e.device().write_at(lba, &replacement).unwrap();
    e.put(b"extra", b"tail", 3).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    assert!(e.dealloc_step(256).segments > 0);
    let mut read = vec![0; replacement.len()];
    e.device().read_at(lba, &mut read).unwrap();
    assert_eq!(read, replacement);
    let disk = e.device().share();
    drop(e);
    let reopened = Engine::open(disk, config()).unwrap();
    check_keys(&reopened);
    reopened.device().read_at(lba, &mut read).unwrap();
    assert_eq!(read, replacement);
}

#[test]
fn kill_switch_discards_pending_work() {
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 4096), config());
    populate(&mut e);
    e.set_device_dealloc(true);
    e.compact_runs(RunBuildOpts::default(), None).unwrap();
    e.set_device_dealloc(false);
    e.put(b"extra", b"tail", 3).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    assert_eq!(e.dealloc_step(256), DeallocStepStats::default());
    e.set_device_dealloc(true);
    assert_eq!(e.dealloc_step(256), DeallocStepStats::default());
    check_keys(&e);
}

#[test]
fn unsupported_disables_tracking_and_errors_are_advisory() {
    for unsupported in [true, false] {
        let mut e = Engine::create_prewiped(DeallocFailDevice {
            inner: MemBlockDevice::new(BS, 4096), unsupported,
        }, config());
        populate(&mut e);
        e.set_device_dealloc(true);
        e.compact_runs(RunBuildOpts::default(), None).unwrap();
        e.put(b"extra", b"tail", 3).unwrap();
        e.flush(RunBuildOpts::default()).unwrap();
        let stats = e.dealloc_step(256);
        assert_eq!(stats.unsupported, unsupported);
        assert_eq!(stats.errors, u64::from(!unsupported));
        assert_eq!(e.alloc.dealloc_tracking(), !unsupported);
        assert_eq!(e.dealloc_step(256), DeallocStepStats::default());
        assert!(!e.poisoned);
        check_keys(&e);
    }
}

struct DeallocFailDevice {
    inner: MemBlockDevice,
    unsupported: bool,
}

impl BlockDevice for DeallocFailDevice {
    fn block_size(&self) -> usize { self.inner.block_size() }
    fn num_blocks(&self) -> u64 { self.inner.num_blocks() }
    fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> { self.inner.read_at(lba, buf) }
    fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> { self.inner.write_at(lba, buf) }
    fn flush(&self) -> vals_spdk::Result<()> { self.inner.flush() }
    fn deallocate(&self, _: &[(u64, u64)]) -> vals_spdk::Result<bool> {
        if self.unsupported { Ok(false) } else { Err(BlockError::Io(std::io::Error::other("injected deallocate error"))) }
    }
}
