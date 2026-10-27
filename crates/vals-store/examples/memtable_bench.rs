use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use vals_store::alloc::SegmentId;
use vals_store::index::{FrontCodedIndex, Index, IndexEntry};
use vals_store::vlog::ValuePtr;
use vals_store::SnapshotEntry;

struct Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static REALLOCS: AtomicU64 = AtomicU64::new(0);
static LIVE: AtomicI64 = AtomicI64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        LIVE.fetch_add(l.size() as i64, Relaxed);
        // SAFETY: forwarded verbatim to the system allocator.
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size() as i64, Relaxed);
        // SAFETY: forwarded verbatim to the system allocator.
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        REALLOCS.fetch_add(1, Relaxed);
        LIVE.fetch_add(n as i64 - l.size() as i64, Relaxed);
        // SAFETY: forwarded verbatim to the system allocator.
        unsafe { System.realloc(p, l, n) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

#[derive(Clone, Copy)]
struct Counts {
    allocs: u64,
    reallocs: u64,
}

fn counts() -> Counts {
    Counts {
        allocs: ALLOCS.load(Relaxed),
        reallocs: REALLOCS.load(Relaxed),
    }
}

fn since(c: Counts) -> Counts {
    let n = counts();
    Counts {
        allocs: n.allocs - c.allocs,
        reallocs: n.reallocs - c.reallocs,
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn skewed(&mut self, n: usize) -> usize {
        let a = self.below(n);
        let b = self.below(n);
        a.min(b)
    }
}

const B32: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
const B32_SORT: &[u8] = b"234567abcdefghijklmnopqrstuvwxyz";

fn did(rng: &mut Rng) -> String {
    let mut s = String::from("did:plc:");
    for _ in 0..24 {
        s.push(B32[rng.below(32)] as char);
    }
    s
}

fn tid(rng: &mut Rng) -> String {
    let micros = 1_733_000_000_000_000u64 + rng.below(86_400_000_000) as u64;
    let v = (micros << 10) | rng.below(1024) as u64;
    let mut out = [0u8; 13];
    for (i, c) in out.iter_mut().enumerate() {
        let shift = 60 - 5 * i as u32;
        *c = B32_SORT[((v >> shift) & 31) as usize];
    }
    String::from_utf8(out.to_vec()).unwrap()
}

struct Shape {
    dids: Vec<String>,
    posts: Vec<String>,
}

impl Shape {
    fn new(rng: &mut Rng) -> Shape {
        let dids: Vec<String> = (0..200_000).map(|_| did(rng)).collect();
        let posts = (0..50_000)
            .map(|_| {
                let d = &dids[rng.below(dids.len())];
                format!("at://{d}/app.bsky.feed.post/{}", tid(rng))
            })
            .collect();
        Shape { dids, posts }
    }

    fn record(&self, rng: &mut Rng) -> Vec<u8> {
        let coll = match rng.below(100) {
            0..=49 => "app.bsky.feed.like",
            50..=69 => "app.bsky.feed.post",
            70..=84 => "app.bsky.graph.follow",
            _ => "app.bsky.feed.repost",
        };
        let d = &self.dids[rng.skewed(self.dids.len())];
        format!("{coll}/{d}/{}", tid(rng)).into_bytes()
    }

    fn backlink(&self, rng: &mut Rng) -> Vec<u8> {
        let target = &self.posts[rng.skewed(self.posts.len())];
        let src = &self.dids[rng.below(self.dids.len())];
        let (coll, path) = match rng.below(100) {
            0..=69 => ("app.bsky.feed.like", "subject.uri"),
            70..=84 => ("app.bsky.feed.repost", "subject.uri"),
            85..=94 => ("app.bsky.feed.post", "reply.parent.uri"),
            _ => ("app.bsky.feed.post", "embed.record.uri"),
        };
        let mut k = target.clone().into_bytes();
        k.push(0);
        k.extend_from_slice(format!("at://{src}/{coll}/{}", tid(rng)).as_bytes());
        k.push(0);
        k.extend_from_slice(path.as_bytes());
        k
    }
}

fn gen_keys(n: usize, seed: u64, backlinks: bool) -> Vec<Vec<u8>> {
    let mut rng = Rng(seed);
    let shape = Shape::new(&mut rng);
    let mut seen = HashSet::with_capacity(n);
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        let k = if backlinks {
            shape.backlink(&mut rng)
        } else {
            shape.record(&mut rng)
        };
        if seen.insert(k.clone()) {
            out.push(k);
        }
    }
    out
}

fn entry(i: u64) -> IndexEntry {
    IndexEntry {
        ptr: ValuePtr {
            segment: SegmentId(i % 4096),
            offset: (i as u32).wrapping_mul(97),
            len: 200 + (i % 300) as u32,
        },
        seq: i + 1,
        version: ((i as u128) << 64) | 7,
        tombstone: false,
    }
}

#[derive(Default, Clone, Copy)]
struct Sample {
    put_ns: f64,
    put_allocs: f64,
    put_reallocs: f64,
    heap_per_entry: f64,
    get_hit_ns: f64,
    get_miss_ns: f64,
    rm_ns: f64,
    rm_allocs: f64,
    rm_reallocs: f64,
}

const FLUSH_SLICE: usize = 512;

#[derive(Clone, Copy, PartialEq)]
enum Commit {
    Batch,
    Fused,
    Legacy,
}

fn flush_commit(idx: &mut FrontCodedIndex, snap: &mut Vec<SnapshotEntry>, mode: Commit) -> usize {
    let mut removed = 0usize;
    while !snap.is_empty() {
        if mode == Commit::Batch {
            let start = snap.len().saturating_sub(FLUSH_SLICE);
            removed += idx.remove_if_eq_rev(&snap[start..]);
            snap.truncate(start);
            continue;
        }
        for _ in 0..FLUSH_SLICE {
            let Some(e) = snap.pop() else { break };
            let captured = IndexEntry {
                ptr: e.ptr,
                seq: e.seq,
                version: e.version,
                tombstone: e.tombstone,
            };
            if mode == Commit::Legacy {
                if idx.get(&e.key) == Some(captured) {
                    idx.remove(&e.key);
                    removed += 1;
                }
            } else if idx.remove_if_eq(&e.key, &captured) {
                removed += 1;
            }
        }
    }
    removed
}

fn run_once(keys: &[Vec<u8>], misses: &[Vec<u8>], order: &[usize], mode: Commit) -> Sample {
    let n = keys.len();
    let live0 = LIVE.load(Relaxed);
    let mut idx = FrontCodedIndex::new();
    let c0 = counts();
    let t = Instant::now();
    for (i, k) in keys.iter().enumerate() {
        idx.put(k, entry(i as u64));
    }
    let put = t.elapsed();
    let pc = since(c0);
    let heap = (LIVE.load(Relaxed) - live0) as f64;
    assert_eq!(idx.len(), n);

    let t = Instant::now();
    let mut hits = 0usize;
    for &i in order {
        hits += idx.get(&keys[i]).is_some() as usize;
    }
    let get_hit = t.elapsed();
    assert_eq!(hits, n);
    let t = Instant::now();
    let mut found = 0usize;
    for k in misses {
        found += idx.get(k).is_some() as usize;
    }
    let get_miss = t.elapsed();
    assert_eq!(found, 0);

    let mut snap: Vec<SnapshotEntry> = idx
        .scan(None, None)
        .unwrap()
        .into_iter()
        .map(|(key, e)| SnapshotEntry {
            key,
            ptr: e.ptr,
            seq: e.seq,
            version: e.version,
            tombstone: e.tombstone,
        })
        .collect();
    assert_eq!(snap.len(), n);
    let c0 = counts();
    let t = Instant::now();
    let removed = flush_commit(&mut idx, &mut snap, mode);
    let rm = t.elapsed();
    let rc = since(c0);
    assert_eq!(removed, n);
    assert!(idx.is_empty());

    let nf = n as f64;
    Sample {
        put_ns: put.as_nanos() as f64 / nf,
        put_allocs: pc.allocs as f64 / nf,
        put_reallocs: pc.reallocs as f64 / nf,
        heap_per_entry: heap / nf,
        get_hit_ns: get_hit.as_nanos() as f64 / nf,
        get_miss_ns: get_miss.as_nanos() as f64 / misses.len() as f64,
        rm_ns: rm.as_nanos() as f64 / nf,
        rm_allocs: rc.allocs as f64 / nf,
        rm_reallocs: rc.reallocs as f64 / nf,
    }
}

fn range_stats(keys: &[Vec<u8>], order: &[usize], ranges: usize, reps: usize) {
    let mut idx = FrontCodedIndex::new();
    for &i in order {
        idx.put(&keys[i], entry(i as u64));
    }
    let mut sorted: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    sorted.sort_unstable();
    let bounds: Vec<Option<&[u8]>> = (0..=ranges)
        .map(|r| (r > 0 && r < ranges).then(|| sorted[r * sorted.len() / ranges]))
        .collect();
    let poll = |materialize: bool| -> (u64, f64, Counts) {
        let before = counts();
        let started = Instant::now();
        let mut total = 0u64;
        for w in bounds.windows(2) {
            let (n, mid) = if materialize {
                let mem = idx.scan(w[0], w[1]).unwrap();
                let mid = (mem.len() >= 2).then(|| mem[mem.len() / 2].0.clone());
                (mem.len(), mid)
            } else {
                idx.range_count_median(w[0], w[1]).unwrap()
            };
            total += n as u64 + mid.map_or(0, |m| m.len() as u64);
        }
        (total, started.elapsed().as_nanos() as f64, since(before))
    };
    let n = keys.len() as f64;
    let heap = idx.allocated_bytes();
    for (label, materialize) in [("scan", true), ("count", false)] {
        let runs: Vec<(u64, f64, Counts)> = (0..reps).map(|_| poll(materialize)).collect();
        let ns = median(runs.iter().map(|r| r.1).collect());
        println!(
            "{:<6} {:>10} {:>7} {:>12} {:>12.3} {:>10.2} {:>12} {:>14}",
            label, keys.len(), ranges, heap, ns / 1e6, ns / n, runs[0].2.allocs, runs[0].0
        );
    }
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    let mut n = 250_000usize;
    let mut reps = 5usize;
    let mut shapes = vec!["records", "backlinks"];
    let mut mode = Commit::Batch;
    let mut ranges = 0usize;
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--n" => n = it.next().unwrap().parse().unwrap(),
            "--reps" => reps = it.next().unwrap().parse().unwrap(),
            "--shape" => shapes = vec![if it.next().unwrap() == "backlinks" { "backlinks" } else { "records" }],
            "--legacy-commit" => mode = Commit::Legacy,
            "--fused-commit" => mode = Commit::Fused,
            "--range-stats" => ranges = it.next().unwrap().parse().unwrap(),
            other => panic!("unknown arg {other}"),
        }
    }
    if ranges > 0 {
        println!(
            "{:<6} {:>10} {:>7} {:>12} {:>12} {:>10} {:>12} {:>14}",
            "poll", "entries", "ranges", "index_B", "ms/poll", "ns/entry", "allocs/poll", "checksum"
        );
        let all = gen_keys(n, 0x5eed_0415, shapes[0] == "backlinks");
        let mut rng = Rng(42);
        let mut order: Vec<usize> = (0..n).collect();
        for i in (1..n).rev() {
            order.swap(i, rng.below(i + 1));
        }
        range_stats(&all, &order, ranges, reps);
        return;
    }
    println!(
        "{:<10} {:>8} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>8}",
        "shape", "put_ns", "put_allc", "put_rall", "B/entry", "hit_ns", "miss_ns", "rm_ns", "rm_allc", "rm_rall", "key_B"
    );
    for shape in shapes {
        let backlinks = shape == "backlinks";
        let all = gen_keys(n + n / 4, 0x5eed_0313, backlinks);
        let (keys, misses) = all.split_at(n);
        let key_b = keys.iter().map(|k| k.len()).sum::<usize>() as f64 / n as f64;
        let mut rng = Rng(42);
        let mut order: Vec<usize> = (0..n).collect();
        for i in (1..n).rev() {
            order.swap(i, rng.below(i + 1));
        }
        let samples: Vec<Sample> = (0..reps).map(|_| run_once(keys, misses, &order, mode)).collect();
        let m = |f: fn(&Sample) -> f64| median(samples.iter().map(f).collect());
        println!(
            "{:<10} {:>8.1} {:>9.3} {:>9.3} {:>9.1} {:>9.1} {:>9.1} {:>9.1} {:>9.3} {:>9.3} {:>8.1}",
            shape,
            m(|s| s.put_ns),
            m(|s| s.put_allocs),
            m(|s| s.put_reallocs),
            m(|s| s.heap_per_entry),
            m(|s| s.get_hit_ns),
            m(|s| s.get_miss_ns),
            m(|s| s.rm_ns),
            m(|s| s.rm_allocs),
            m(|s| s.rm_reallocs),
            key_b
        );
    }
}
