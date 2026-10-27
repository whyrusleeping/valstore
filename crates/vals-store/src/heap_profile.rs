use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};

struct Profiling;

#[global_allocator]
static GLOBAL: Profiling = Profiling;
static LIVE: AtomicU64 = AtomicU64::new(0);
static COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static REALLOCS: AtomicU64 = AtomicU64::new(0);
static INTERNAL: AtomicU64 = AtomicU64::new(0);
static ENABLED: AtomicBool = AtomicBool::new(false);
static INTERVAL: AtomicUsize = AtomicUsize::new(128 << 10);

thread_local! {
    static BUSY: Cell<bool> = const { Cell::new(false) };
    static LEFT: Cell<usize> = const { Cell::new(0) };
    static RNG: Cell<u64> = const { Cell::new(0x397a93b0471) };
}

struct Guard;

impl Guard {
    fn enter() -> Option<Self> {
        BUSY.try_with(|busy| if busy.replace(true) { None } else { Some(Self) }).ok().flatten()
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let _ = BUSY.try_with(|busy| busy.set(false));
    }
}

#[derive(Clone, Copy)]
struct Sample {
    size: usize,
    weight: usize,
    stack: usize,
}

#[derive(Default)]
struct Samples {
    live: HashMap<usize, Sample>,
    stacks: Vec<Vec<usize>>,
    ids: HashMap<Vec<usize>, usize>,
}

fn samples() -> &'static Mutex<Samples> {
    static SAMPLES: OnceLock<Mutex<Samples>> = OnceLock::new();
    SAMPLES.get_or_init(|| Mutex::new(Samples::default()))
}

fn internal() -> bool {
    BUSY.try_with(Cell::get).unwrap_or(true)
}

fn record(ptr: *mut u8, size: usize) {
    if !ENABLED.load(Relaxed) { return; }
    let Some(_guard) = Guard::enter() else { return; };
    let interval = INTERVAL.load(Relaxed);
    let sampled = LEFT.with(|left| {
        let remaining = left.get();
        if size >= interval || size >= remaining {
            let next = RNG.with(|rng| {
                let x = crate::run::splitmix64(rng.get());
                rng.set(x);
                (x as usize % (2 * interval - 1)) + 1
            });
            left.set(next);
            true
        } else {
            left.set(remaining - size);
            false
        }
    });
    if !sampled { return; }
    let mut stack = Vec::with_capacity(32);
    backtrace::trace(|frame| {
        stack.push(frame.ip() as usize);
        stack.len() < 40
    });
    let mut samples = samples().lock().unwrap();
    let id = match samples.ids.get(&stack) {
        Some(id) => *id,
        None => {
            let id = samples.stacks.len();
            samples.stacks.push(stack.clone());
            samples.ids.insert(stack, id);
            id
        }
    };
    samples.live.insert(ptr as usize, Sample { size, weight: size.max(interval), stack: id });
}

fn remove(ptr: *mut u8) -> Option<Sample> {
    if !ENABLED.load(Relaxed) { return None; }
    let _guard = Guard::enter()?;
    samples().lock().unwrap().live.remove(&(ptr as usize))
}

unsafe impl GlobalAlloc for Profiling {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: layout is forwarded unchanged to System.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE.fetch_add(layout.size() as u64, Relaxed);
            COUNT.fetch_add(1, Relaxed);
            ALLOCS.fetch_add(1, Relaxed);
            if internal() { INTERNAL.fetch_add(layout.size() as u64, Relaxed); }
            record(ptr, layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: layout is forwarded unchanged to System.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            LIVE.fetch_add(layout.size() as u64, Relaxed);
            COUNT.fetch_add(1, Relaxed);
            ALLOCS.fetch_add(1, Relaxed);
            if internal() { INTERNAL.fetch_add(layout.size() as u64, Relaxed); }
            record(ptr, layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as u64, Relaxed);
        COUNT.fetch_sub(1, Relaxed);
        if internal() { INTERNAL.fetch_sub(layout.size() as u64, Relaxed); }
        remove(ptr);
        // SAFETY: pointer and layout are forwarded unchanged to System.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let previous = remove(ptr);
        // SAFETY: pointer, layout and new size are forwarded unchanged to System.
        let out = unsafe { System.realloc(ptr, layout, size) };
        if !out.is_null() {
            LIVE.fetch_add((size as u64).wrapping_sub(layout.size() as u64), Relaxed);
            REALLOCS.fetch_add(1, Relaxed);
            if internal() { INTERNAL.fetch_add((size as u64).wrapping_sub(layout.size() as u64), Relaxed); }
            record(out, size);
        } else if let Some(sample) = previous {
            let _guard = Guard::enter().unwrap();
            samples().lock().unwrap().live.insert(ptr as usize, sample);
        }
        out
    }
}

pub(crate) fn enable() {
    let _guard = Guard::enter().unwrap();
    let interval = std::env::var("VALS_HEAP_SAMPLE_BYTES").ok().and_then(|v| v.parse().ok()).unwrap_or(128 << 10);
    assert!((1024..=1 << 30).contains(&interval));
    INTERVAL.store(interval, Relaxed);
    ENABLED.store(true, Relaxed);
    backtrace::resolve(enable as *mut std::ffi::c_void, |_| {});
}

pub(crate) fn counts() -> (u64, u64, u64, u64, u64) {
    (LIVE.load(Relaxed), COUNT.load(Relaxed), ALLOCS.load(Relaxed), REALLOCS.load(Relaxed), INTERNAL.load(Relaxed))
}

pub(crate) fn dump(label: &str) {
    let _guard = Guard::enter().unwrap();
    let samples = samples().lock().unwrap();
    let mut totals: HashMap<usize, (u64, u64, u64)> = HashMap::new();
    for sample in samples.live.values() {
        let total = totals.entry(sample.stack).or_default();
        total.0 += sample.weight as u64;
        total.1 += sample.size as u64;
        total.2 += 1;
    }
    let mut collapsed: HashMap<String, (u64, u64, u64)> = HashMap::new();
    for (id, total) in totals {
        let mut names = Vec::new();
        for ip in &samples.stacks[id] {
            backtrace::resolve(*ip as *mut std::ffi::c_void, |symbol| {
                if let Some(name) = symbol.name() {
                    let name = name.to_string();
                    if name.contains("vals_store::") && !name.contains("heap_profile") && names.len() < 4 {
                        names.push(name);
                    }
                }
            });
        }
        let name = if names.is_empty() { "runtime/other".to_owned() } else { names.join(" <- ") };
        let entry = collapsed.entry(name).or_default();
        entry.0 += total.0;
        entry.1 += total.1;
        entry.2 += total.2;
    }
    let estimated: u64 = collapsed.values().map(|v| v.0).sum();
    let mut rows: Vec<_> = collapsed.into_iter().collect();
    rows.sort_unstable_by_key(|row| std::cmp::Reverse(row.1.0));
    eprintln!("HP397 label={label} interval={} samples={} stacks={} estimated={estimated}", INTERVAL.load(Relaxed), samples.live.len(), samples.stacks.len());
    for (name, (bytes, actual, count)) in rows.into_iter().take(20) {
        eprintln!("HP397 stack label={label} estimated={bytes} sampled={actual} count={count} share={:.6} frames={name}", bytes as f64 / estimated.max(1) as f64);
    }
}

#[test]
fn tracks_live_allocations_and_reallocations() {
    if std::env::var_os("VALS_HEAP_TEST_CHILD").is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "heap_profile::tracks_live_allocations_and_reallocations", "--nocapture"])
            .env("VALS_HEAP_TEST_CHILD", "1").status().unwrap();
        assert!(status.success());
        return;
    }
    let _guard = Guard::enter().unwrap();
    let before = counts();
    let layout = Layout::from_size_align(16384, 8).unwrap();
    // SAFETY: the allocation uses a valid layout, and each pointer is used only until reallocation or deallocation.
    unsafe {
        let ptr = GLOBAL.alloc(layout);
        assert!(!ptr.is_null());
        assert_eq!(counts().0 - before.0, 16384);
        assert_eq!(counts().1 - before.1, 1);
        let ptr = GLOBAL.realloc(ptr, layout, 32768);
        assert!(!ptr.is_null());
        assert_eq!(counts().0 - before.0, 32768);
        GLOBAL.dealloc(ptr, Layout::from_size_align(32768, 8).unwrap());
    }
    assert_eq!(counts().0, before.0);
    assert_eq!(counts().1, before.1);
}
