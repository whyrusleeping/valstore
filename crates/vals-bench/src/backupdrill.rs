use std::io::Write;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering as AtOrd};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use vals_cluster::{reactor, Coordinator, NetTransport, Persistence, Ring, WriteConcern, WriteOptions, ZoneId};

const USAGE: &str = "usage: vals-bench vals backupdrill populate <peers> <keys-per-zone> [threads]\n       \
     vals-bench vals backupdrill verify <peers> <keys-per-zone> [threads]\n       \
     vals-bench vals backupdrill probe <peers> <secs> <rate-per-sec> <acked-log>\n       \
     vals-bench vals backupdrill rpo <peers> <acked-log>\n\
     zones: default (0), bd_unsorted, bd_sorted (sorted) hold the model; bd_probe (sorted) takes probe writes";

const CHUNK: usize = 256;
const RANGE_BLOCK: usize = 1000;
const RANGE_LO: usize = 100;
const RANGE_HI: usize = 200;
const REWRITE_EVERY: usize = 50;

fn run<F: std::future::Future>(f: F) -> F::Output {
    vals_rt::block_on(f, reactor::poll_io)
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

fn parse_peers(spec: &str) -> Vec<(u32, Vec<SocketAddr>)> {
    spec.split(';')
        .filter(|s| !s.trim().is_empty())
        .map(|s| {
            let (id, addrs) = s.split_once('@').expect("peer must be id@addr[,addr...]");
            let addrs = addrs.split(',').filter(|a| !a.trim().is_empty()).map(|a| a.trim().parse().expect("ip:port")).collect();
            (id.trim().parse().expect("peer id"), addrs)
        })
        .collect()
}

fn coordinator(nodes: &[(u32, Vec<SocketAddr>)], id: u32) -> Coordinator<NetTransport> {
    let mut ring = Ring::new(nodes.len().clamp(1, 3));
    ring.set_members_weighted(nodes.iter().map(|(node, _)| (*node, 1)));
    let coord = Coordinator::new(id, ring, 2, NetTransport::connect_sharded(nodes.iter().cloned()));
    let deadline = Instant::now() + Duration::from_secs(60);
    while !run(coord.adopt_from_any_member()) {
        assert!(Instant::now() < deadline, "backupdrill: could not adopt the ring and catalog from any node");
        std::thread::sleep(Duration::from_millis(200));
    }
    coord
}

fn zone(coord: &Coordinator<NetTransport>, name: &str) -> ZoneId {
    if name == "default" {
        return vals_cluster::DEFAULT_ZONE;
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(id) = coord.catalog().by_name(name) {
            return id;
        }
        assert!(Instant::now() < deadline, "backupdrill: zone {name:?} is not in the catalog; create it first");
        let _ = run(coord.adopt_from_any_member());
        std::thread::sleep(Duration::from_millis(200));
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Default,
    Unsorted,
    Sorted,
}

const ZONES: [(&str, Kind, u64); 3] =
    [("default", Kind::Default, 0x11), ("bd_unsorted", Kind::Unsorted, 0x22), ("bd_sorted", Kind::Sorted, 0x33)];

fn key(i: usize) -> Vec<u8> {
    format!("k{i:010}").into_bytes()
}

fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn value(tag: u64, i: usize, generation: u64) -> Vec<u8> {
    let mut state = mix(tag << 56 ^ (i as u64) << 8 ^ generation);
    let len = 64 + (state % 961) as usize;
    let mut out = Vec::with_capacity(len + 8);
    out.extend_from_slice(format!("g{generation}:").as_bytes());
    while out.len() < len {
        state = mix(state);
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(len);
    out
}

fn overwritten(i: usize) -> bool {
    i % 3 == 0
}

fn point_deleted(i: usize) -> bool {
    i % 7 == 3
}

fn range_blocks(n: usize) -> impl Iterator<Item = usize> {
    (0..n / RANGE_BLOCK).filter(|block| block % 10 == 5)
}

fn range_deleted(i: usize) -> bool {
    let block = i / RANGE_BLOCK;
    block % 10 == 5 && (RANGE_LO..RANGE_HI).contains(&(i % RANGE_BLOCK))
}

fn rewritten(i: usize) -> bool {
    range_deleted(i) && i % REWRITE_EVERY == 0
}

fn expected(kind: Kind, tag: u64, i: usize, n: usize) -> Option<Vec<u8>> {
    let in_ranges = kind == Kind::Sorted && i < n / RANGE_BLOCK * RANGE_BLOCK && range_deleted(i);
    if in_ranges {
        return rewritten(i).then(|| value(tag, i, 3));
    }
    if point_deleted(i) {
        return None;
    }
    Some(value(tag, i, if overwritten(i) { 2 } else { 1 }))
}

fn options() -> WriteOptions {
    WriteOptions { concern: WriteConcern::Majority, persistence: Persistence::Buffered }
}

fn retry<T, E: std::fmt::Debug>(what: &str, mut f: impl FnMut() -> Result<T, E>) -> T {
    let mut last = None;
    for attempt in 0..40 {
        match f() {
            Ok(value) => return value,
            Err(e) => {
                if attempt % 10 == 9 {
                    eprintln!("backupdrill: {what}: {e:?} (retrying)");
                }
                last = Some(e);
                std::thread::sleep(Duration::from_millis(50 * (attempt + 1).min(20)));
            }
        }
    }
    panic!("backupdrill: {what}: {last:?}");
}

fn put_chunk(coord: &Coordinator<NetTransport>, zone_id: ZoneId, tag: u64, indices: &[usize], generation: u64) {
    if indices.is_empty() {
        return;
    }
    let owned: Vec<(Vec<u8>, Vec<u8>)> = indices.iter().map(|&i| (key(i), value(tag, i, generation))).collect();
    let writes: Vec<(&[u8], &[u8])> = owned.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
    let mut pending: Vec<usize> = (0..writes.len()).collect();
    retry("batch write", || {
        let batch: Vec<(&[u8], &[u8])> = pending.iter().map(|&j| writes[j]).collect();
        let result = run(coord.write_batch_in(zone_id, &batch, options())).map_err(|e| format!("{e:?}"))?;
        let failed: Vec<usize> = result.failures().map(|(j, _)| pending[j]).collect();
        if failed.is_empty() {
            return Ok(());
        }
        let first = result.failures().next().map(|(_, e)| format!("{e:?}")).unwrap_or_default();
        pending = failed;
        Err(format!("{} writes failed, first {first}", pending.len()))
    });
}

fn parallel(threads: usize, chunks: usize, work: impl Fn(&Coordinator<NetTransport>, usize) + Sync, nodes: &[(u32, Vec<SocketAddr>)], done: &AtomicU64) {
    std::thread::scope(|scope| {
        for t in 0..threads {
            let work = &work;
            scope.spawn(move || {
                let coord = coordinator(nodes, 4_000_000 + t as u32);
                for chunk in (t..chunks).step_by(threads) {
                    work(&coord, chunk);
                    done.fetch_add(1, AtOrd::Relaxed);
                }
            });
        }
    });
}

fn phase(name: &str, total: usize, threads: usize, nodes: &[(u32, Vec<SocketAddr>)], work: impl Fn(&Coordinator<NetTransport>, usize) + Sync) {
    struct Finished<'a>(&'a std::sync::atomic::AtomicBool);
    impl Drop for Finished<'_> {
        fn drop(&mut self) {
            self.0.store(true, AtOrd::Relaxed);
        }
    }
    assert!(threads > 0, "backupdrill: threads must be at least 1");
    let start = Instant::now();
    let done = AtomicU64::new(0);
    let finished = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        let reporter = scope.spawn(|| {
            let mut last = 0;
            while !finished.load(AtOrd::Relaxed) {
                std::thread::sleep(Duration::from_millis(200));
                let secs = start.elapsed().as_secs();
                if secs >= last + 10 {
                    last = secs;
                    eprintln!("backupdrill: {name}: {}/{total} chunks after {secs}s", done.load(AtOrd::Relaxed));
                }
            }
        });
        let finish = Finished(&finished);
        parallel(threads, total, work, nodes, &done);
        drop(finish);
        reporter.join().unwrap();
    });
    println!(
        "BD_PHASE {{\"phase\":\"{name}\",\"chunks\":{total},\"secs\":{:.1}}}",
        start.elapsed().as_secs_f64()
    );
}

fn populate(nodes: &[(u32, Vec<SocketAddr>)], n: usize, threads: usize) {
    let coord = coordinator(nodes, 3_999_999);
    let zones: Vec<(ZoneId, Kind, u64)> = ZONES.iter().map(|&(name, kind, tag)| (zone(&coord, name), kind, tag)).collect();
    let chunks = n.div_ceil(CHUNK);
    let indices = |chunk: usize| chunk * CHUNK..((chunk + 1) * CHUNK).min(n);
    let t0 = now_ms();
    phase("put", chunks * zones.len(), threads, nodes, |coord, c| {
        let (zone_id, _, tag) = zones[c % zones.len()];
        let all: Vec<usize> = indices(c / zones.len()).collect();
        put_chunk(coord, zone_id, tag, &all, 1);
    });
    phase("overwrite", chunks * zones.len(), threads, nodes, |coord, c| {
        let (zone_id, _, tag) = zones[c % zones.len()];
        let some: Vec<usize> = indices(c / zones.len()).filter(|&i| overwritten(i)).collect();
        put_chunk(coord, zone_id, tag, &some, 2);
    });
    phase("delete", chunks * zones.len(), threads, nodes, |coord, c| {
        let (zone_id, _, _) = zones[c % zones.len()];
        let keys: Vec<Vec<u8>> = indices(c / zones.len()).filter(|&i| point_deleted(i)).map(key).collect();
        let deletes = keys.iter().map(|k| async move {
            (k, coord.delete_in(zone_id, k, options()).await)
        });
        for (k, result) in run(vals_rt::join_all(deletes.collect())) {
            if result.is_err() {
                retry("delete", || run(coord.delete_in(zone_id, k, options())));
            }
        }
    });
    let (sorted, _, tag) = *zones.iter().find(|(_, kind, _)| *kind == Kind::Sorted).unwrap();
    let blocks: Vec<usize> = range_blocks(n).collect();
    phase("range-delete", blocks.len(), threads, nodes, |coord, b| {
        let block = blocks[b];
        let (lo, hi) = (key(block * RANGE_BLOCK + RANGE_LO), key(block * RANGE_BLOCK + RANGE_HI));
        retry("range delete", || run(coord.delete_range_in(sorted, Some(&lo), Some(&hi), options())));
        let again: Vec<usize> = (block * RANGE_BLOCK + RANGE_LO..block * RANGE_BLOCK + RANGE_HI).filter(|&i| rewritten(i)).collect();
        put_chunk(coord, sorted, tag, &again, 3);
    });
    let mut counts = [0u64; 2];
    for &(_, kind, tag) in &zones {
        for i in 0..n {
            counts[expected(kind, tag, i, n).is_some() as usize] += 1;
        }
    }
    println!(
        "BD_POPULATED {{\"keys_per_zone\":{n},\"zones\":{},\"live\":{},\"deleted\":{},\"range_deletes\":{},\"start_ms\":{t0},\"end_ms\":{}}}",
        zones.len(),
        counts[1],
        counts[0],
        blocks.len(),
        now_ms()
    );
}

fn verify(nodes: &[(u32, Vec<SocketAddr>)], n: usize, threads: usize) {
    let coord = coordinator(nodes, 3_999_998);
    let zones: Vec<(ZoneId, Kind, u64)> = ZONES.iter().map(|&(name, kind, tag)| (zone(&coord, name), kind, tag)).collect();
    let chunks = n.div_ceil(CHUNK);
    let checked = AtomicU64::new(0);
    let mismatches = AtomicU64::new(0);
    let samples = std::sync::Mutex::new(Vec::new());
    phase("verify-reads", chunks * zones.len(), threads, nodes, |coord, c| {
        let (zone_id, kind, tag) = zones[c % zones.len()];
        let range: Vec<usize> = (c / zones.len() * CHUNK..((c / zones.len() + 1) * CHUNK).min(n)).collect();
        let keys: Vec<Vec<u8>> = range.iter().map(|&i| key(i)).collect();
        let got = retry("read", || {
            let replies = run(coord.read_many_in(zone_id, &keys)).map_err(|e| format!("{e:?}"))?;
            replies.into_iter().collect::<Result<Vec<_>, _>>().map_err(|e| format!("{e:?}"))
        });
        for (&i, stored) in range.iter().zip(got) {
            let want = expected(kind, tag, i, n);
            let have = stored.and_then(|s| s.value);
            checked.fetch_add(1, AtOrd::Relaxed);
            if have != want {
                mismatches.fetch_add(1, AtOrd::Relaxed);
                let mut samples = samples.lock().unwrap();
                if samples.len() < 20 {
                    samples.push(format!(
                        "zone {zone_id} key {i}: want {:?} bytes, have {:?} bytes",
                        want.as_ref().map(Vec::len),
                        have.as_ref().map(Vec::len)
                    ));
                }
            }
        }
    });
    let (sorted, _, tag) = *zones.iter().find(|(_, kind, _)| *kind == Kind::Sorted).unwrap();
    let start = Instant::now();
    let mut lo = key(0);
    let mut scanned = 0u64;
    let mut scan_mismatch = 0u64;
    let mut want = (0..n).filter(|&i| expected(Kind::Sorted, tag, i, n).is_some());
    loop {
        let page = retry("scan", || run(coord.scan_in(sorted, Some(&lo), None, 2, Some(4096))).map_err(|e| format!("{e:?}")));
        let Some((last, _)) = page.last() else { break };
        let next = {
            let mut next = last.clone();
            next.push(0);
            next
        };
        for (k, v) in &page {
            scanned += 1;
            let ok = want.next().is_some_and(|i| key(i) == *k && Some(v.clone()) == expected(Kind::Sorted, tag, i, n));
            if !ok {
                scan_mismatch += 1;
                let mut samples = samples.lock().unwrap();
                if samples.len() < 20 {
                    samples.push(format!("scan of zone {sorted}: unexpected key {}", String::from_utf8_lossy(k)));
                }
            }
        }
        lo = next;
    }
    let missing_from_scan = want.count() as u64;
    let total_mismatch = mismatches.load(AtOrd::Relaxed) + scan_mismatch + missing_from_scan;
    println!(
        "BD_VERIFY {{\"checked\":{},\"mismatches\":{},\"scanned\":{scanned},\"scan_mismatches\":{scan_mismatch},\"scan_missing\":{missing_from_scan},\"scan_secs\":{:.1}}}",
        checked.load(AtOrd::Relaxed),
        mismatches.load(AtOrd::Relaxed),
        start.elapsed().as_secs_f64()
    );
    for sample in samples.lock().unwrap().iter() {
        println!("  {sample}");
    }
    if total_mismatch > 0 {
        std::process::exit(1);
    }
}

#[derive(Default)]
struct Lat(Vec<u64>);

impl Lat {
    fn json(&mut self) -> String {
        self.0.sort_unstable();
        let at = |q: f64| self.0.get(((self.0.len() as f64 * q) as usize).min(self.0.len().saturating_sub(1))).copied().unwrap_or(0);
        let json = format!(
            "{{\"n\":{},\"p50\":{},\"p99\":{},\"max\":{}}}",
            self.0.len(),
            at(0.5),
            at(0.99),
            self.0.last().copied().unwrap_or(0)
        );
        self.0.clear();
        json
    }
}

fn probe(nodes: &[(u32, Vec<SocketAddr>)], secs: u64, rate: u64, log: &str) {
    let coord = coordinator(nodes, 3_999_997);
    let probe_zone = zone(&coord, "bd_probe");
    let read_zone = zone(&coord, "bd_unsorted");
    let mut acked = std::io::BufWriter::new(std::fs::OpenOptions::new().create(true).append(true).open(log).expect("acked log"));
    let tag = now_ms();
    let mut seq = 0u64;
    let period = Duration::from_micros(1_000_000 / rate.max(1));
    let start = Instant::now();
    let mut next = start;
    let mut window = Instant::now();
    let (mut writes, mut reads) = (Lat::default(), Lat::default());
    let mut errors = 0u64;
    let mut op = 0u64;
    while start.elapsed() < Duration::from_secs(secs) {
        next += period;
        if let Some(wait) = next.checked_duration_since(Instant::now()) {
            std::thread::sleep(wait);
        }
        op += 1;
        let t = Instant::now();
        if op % 2 == 0 {
            let sent = now_ms();
            let k = format!("p{tag}-{seq:09}");
            match run(coord.write_in(probe_zone, k.as_bytes(), sent.to_string().as_bytes(), options())) {
                Ok(_) => {
                    writes.0.push(t.elapsed().as_micros() as u64);
                    writeln!(acked, "{k} {sent} {}", now_ms()).unwrap();
                    seq += 1;
                }
                Err(_) => errors += 1,
            }
        } else {
            let i = (mix(op) % 1_000) as usize;
            match run(coord.read_in(read_zone, &key(i))) {
                Ok(_) => reads.0.push(t.elapsed().as_micros() as u64),
                Err(_) => errors += 1,
            }
        }
        if window.elapsed() >= Duration::from_secs(1) {
            window = Instant::now();
            acked.flush().unwrap();
            println!(
                "BD_PROBE {{\"t_ms\":{},\"write_us\":{},\"read_us\":{},\"errors\":{}}}",
                now_ms(),
                writes.json(),
                reads.json(),
                std::mem::take(&mut errors)
            );
        }
    }
    acked.flush().unwrap();
}

fn rpo(nodes: &[(u32, Vec<SocketAddr>)], log: &str) {
    let coord = coordinator(nodes, 3_999_996);
    let probe_zone = zone(&coord, "bd_probe");
    let mut present = std::collections::BTreeSet::new();
    let mut lo = b"p".to_vec();
    loop {
        let page = retry("scan", || run(coord.scan_in(probe_zone, Some(&lo), None, 2, Some(4096))).map_err(|e| format!("{e:?}")));
        let Some((last, _)) = page.last() else { break };
        lo = last.clone();
        lo.push(0);
        for (k, _) in &page {
            present.insert(String::from_utf8_lossy(k).into_owned());
        }
    }
    let text = std::fs::read_to_string(log).expect("acked log");
    let acked: Vec<(String, u64, u64)> = text
        .lines()
        .filter_map(|line| {
            let mut f = line.split_whitespace();
            Some((f.next()?.to_string(), f.next()?.parse().ok()?, f.next()?.parse().ok()?))
        })
        .collect();
    let missing: Vec<&(String, u64, u64)> = acked.iter().filter(|(seq, _, _)| !present.contains(seq)).collect();
    let first_missing_ack = missing.iter().map(|(_, _, ack)| *ack).min();
    let last_present_ack = acked.iter().filter(|(seq, _, _)| present.contains(seq)).map(|(_, _, ack)| *ack).max();
    let present_after_first_missing = first_missing_ack
        .map_or(0, |at| acked.iter().filter(|(seq, _, ack)| *ack > at && present.contains(seq)).count());
    println!(
        "BD_RPO {{\"acked\":{},\"present\":{},\"missing\":{},\"first_missing_ack_ms\":{},\"last_present_ack_ms\":{},\"present_after_first_missing\":{present_after_first_missing}}}",
        acked.len(),
        acked.len() - missing.len(),
        missing.len(),
        first_missing_ack.map_or("null".into(), |v| v.to_string()),
        last_present_ack.map_or("null".into(), |v| v.to_string()),
    );
}

pub(super) fn workload_backupdrill(args: &[String]) {
    let arg = |i: usize| args.get(i).map(String::as_str);
    let num = |i: usize, default: u64| arg(i).map_or(default, |s| s.parse().expect("number"));
    match (arg(3), arg(4)) {
        (Some("populate"), Some(peers)) => populate(&parse_peers(peers), num(5, 100_000) as usize, num(6, 16) as usize),
        (Some("verify"), Some(peers)) => verify(&parse_peers(peers), num(5, 100_000) as usize, num(6, 16) as usize),
        (Some("probe"), Some(peers)) if arg(7).is_some() => probe(&parse_peers(peers), num(5, 60), num(6, 200), arg(7).unwrap()),
        (Some("rpo"), Some(peers)) if arg(5).is_some() => rpo(&parse_peers(peers), arg(5).unwrap()),
        _ => {
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    }
}
