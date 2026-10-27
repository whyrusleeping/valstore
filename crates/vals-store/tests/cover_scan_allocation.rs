use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine, Ordering, RunBuildOpts};

struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|n| n.set(n.get() + 1));
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|n| n.set(n.get() + 1));
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCATIONS.with(|n| n.set(n.get() + 1));
        unsafe { System.realloc(ptr, layout, size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

#[test]
fn covered_runs_skip_cover_scan_planner_allocation() {
    let mut e = Engine::create_prewiped(
        MemBlockDevice::new(512, 4096),
        Config { seg_blocks: 16, reserved_blocks: 0, ordering: Ordering::Sorted },
    );
    for round in 0..3 {
        e.put(format!("key-{round}").as_bytes(), b"value", 1).unwrap();
        e.flush(RunBuildOpts::default()).unwrap();
    }
    assert_eq!(e.key_filter_stats().covered_runs, 3);
    assert_eq!(e.key_filter_stats().rebuilding, 0);
    let before = ALLOCATIONS.with(Cell::get);
    for _ in 0..100 {
        assert!(e.cover_scan_plan().is_none());
    }
    let after = ALLOCATIONS.with(Cell::get);
    assert_eq!(after - before, 0, "covered-zone maintenance ticks must not allocate");
}
