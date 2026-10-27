use std::net::SocketAddr;

use vals_cluster::{reactor, Coordinator, NetTransport, Ring, Transport};

fn show(k: &[u8]) -> String {
    if k.iter().all(|b| b.is_ascii_graphic()) {
        String::from_utf8_lossy(k).into_owned()
    } else {
        k.iter().map(|b| format!("{b:02x}")).collect()
    }
}

fn opt(k: &Option<Vec<u8>>) -> String {
    k.as_deref().map_or("-".into(), show)
}

fn run<F: std::future::Future>(fut: F) -> F::Output {
    vals_rt::block_on(fut, reactor::poll_io)
}

fn print_page(tag: &str, page: &Result<vals_cluster::coord::ScanPage, vals_cluster::TransportError>, show_n: usize) {
    match page {
        Err(e) => println!("  {tag}: ERR {e:?}"),
        Ok(p) => {
            println!(
                "  {tag}: entries={} more={} resume={} fragments={}",
                p.entries.len(),
                p.more,
                opt(&p.resume),
                p.fragments.len()
            );
            for (k, s) in p.entries.iter().take(show_n) {
                let v = s.value.as_ref().map_or("TOMB".into(), |v| format!("{}B", v.len()));
                println!("    {} v=({},{}) {v}", show(k), s.version.hlc, s.version.node);
            }
            for f in p.fragments.iter().take(show_n) {
                println!("    frag [{}, {}) v=({},{})", opt(&f.lo), opt(&f.hi), f.version.hlc, f.version.node);
            }
        }
    }
}

pub(super) fn workload_rtdiag(args: &[String]) {
    let usage = "usage: vals-bench vals rtdiag <id@a1,a2;id@...> <zone> <lo> <hi> [max-slices] [probe-key]";
    let (Some(spec), Some(zone_name), Some(lo), Some(hi)) = (args.get(3), args.get(4), args.get(5), args.get(6)) else {
        eprintln!("{usage}");
        std::process::exit(2);
    };
    let max_slices: usize = args.get(7).and_then(|s| s.parse().ok()).unwrap_or(2);
    let probe = args.get(8).map(|s| s.as_bytes().to_vec());
    let nodes: Vec<(u32, Vec<SocketAddr>)> = spec
        .split(';')
        .filter(|s| !s.trim().is_empty())
        .map(|g| {
            let (id, addrs) = g.split_once('@').expect("id@addrs");
            (id.trim().parse().expect("id"), addrs.split(',').map(|a| a.trim().parse().expect("addr")).collect())
        })
        .collect();
    let mut ring = Ring::new(nodes.len().clamp(1, 3));
    ring.set_members_weighted(nodes.iter().map(|(id, _)| (*id, 1)));
    let coord = Coordinator::new(4_242_001, ring, 2, NetTransport::connect_sharded(nodes.iter().cloned()));
    assert!(run(coord.adopt_from_any_member()), "adopt failed");
    let from = coord.ring().members().next().expect("members");
    let _ = run(coord.refresh_catalog(from));
    let zone = coord.catalog().by_name(zone_name).expect("zone");
    let (rf, scope) = {
        let cat = coord.catalog();
        let cfg = cat.get(zone).expect("zone cfg");
        (cfg.rf, cfg.scope.clone())
    };
    let (lo, hi) = (lo.as_bytes(), hi.as_bytes());
    for r in 1..=3usize {
        for limit in [1usize, 10, 4096] {
            let res = run(coord.scan_in(zone, Some(lo), Some(hi), r, Some(limit)));
            match res {
                Ok(e) => println!(
                    "coord scan R={r} limit={limit}: n={} first={:?}",
                    e.len(),
                    e.iter().take(3).map(|(k, _)| show(k)).collect::<Vec<_>>()
                ),
                Err(e) => println!("coord scan R={r} limit={limit}: ERR {e}"),
            }
        }
    }
    let epoch = coord.ring_epoch();
    let map = coord.range_map(zone);
    println!("zone {zone_name} id={zone} rf={rf} scope={scope:?} ring_epoch={epoch} ranges={}", map.num_ranges());
    let slices = map.ranges_overlapping(Some(lo), Some(hi));
    println!("slices overlapping: {}", slices.len());
    let shard_tx: Vec<(u32, Vec<NetTransport>)> = nodes
        .iter()
        .map(|(id, addrs)| {
            let txs = addrs
                .iter()
                .map(|a| {
                    let t = NetTransport::connect_sharded([(*id, vec![*a])]);
                    t.set_route_epoch(vals_cluster::wire::INSPECT_EPOCH);
                    t
                })
                .collect();
            (*id, txs)
        })
        .collect();
    let mut targets: Vec<_> = slices.iter().take(max_slices).collect();
    if let Some(p) = &probe {
        if let Some(s) = slices.iter().find(|s| s.lo.as_deref().is_none_or(|l| l <= p.as_slice()) && s.hi.as_deref().is_none_or(|h| p.as_slice() < h)) {
            if !targets.iter().any(|t| t.index == s.index) {
                targets.push(s);
            }
        }
    }
    for s in targets {
        let reps = coord.ring().replicas_in_zone(zone, &s.token, rf, scope.as_ref());
        let shard = vals_cluster::scan_shard(Some(&map), s.lo.as_deref(), 8);
        println!(
            "slice #{} [{}, {}) token={} replicas={reps:?} coord_scan_shard={shard}",
            s.index,
            opt(&s.lo),
            opt(&s.hi),
            s.token.iter().map(|b| format!("{b:02x}")).collect::<String>()
        );
        for &n in &reps {
            for max in [10u32, 4096] {
                let page = run(coord.transport().scan(n, zone, s.lo.as_deref(), s.hi.as_deref(), max));
                print_page(&format!("node {n} routed max={max}"), &page, 6);
            }
        }
        for (n, txs) in &shard_tx {
            for (i, t) in txs.iter().enumerate() {
                let page = run(t.scan(*n, zone, s.lo.as_deref(), s.hi.as_deref(), 10));
                if matches!(&page, Ok(p) if p.entries.is_empty() && p.fragments.is_empty() && !p.more) {
                    continue;
                }
                print_page(&format!("node {n} shard {i} max=10"), &page, 6);
            }
        }
    }
    if let Some(p) = &probe {
        let mut stale = map.clone();
        let (idx, first) = (stale.range_index(p), stale.range_index(lo));
        let old = stale.token_of(first).to_vec();
        if idx != first && stale.set_pending(idx, old) && stale.commit_pending(idx) {
            let sc = Coordinator::new(4_242_002, coord.ring().clone(), 2, NetTransport::connect_sharded(nodes.iter().cloned()))
                .with_manual_refresh();
            assert!(run(sc.adopt_from_any_member()), "adopt failed");
            sc.set_range_map(zone, stale);
            for r in 1..=3usize {
                let res = run(sc.scan_in(zone, Some(lo), Some(hi), r, Some(10)));
                println!(
                    "stale-map coord (range #{idx} on range #{first}'s token) scan R={r} limit=10: {:?}",
                    res.map(|e| e.iter().take(3).map(|(k, _)| show(k)).collect::<Vec<_>>())
                );
            }
        }
    }
    if let Some(p) = probe {
        println!("probe key {}: read_in={:?}", show(&p), run(coord.read_in(zone, &p)).map(|o| o.map(|s| (s.version, s.value.map(|v| v.len())))));
        for (n, txs) in &shard_tx {
            for (i, t) in txs.iter().enumerate() {
                let next = {
                    let mut k = p.clone();
                    k.push(0);
                    k
                };
                let page = run(t.scan(*n, zone, Some(&p), Some(&next), 1));
                if matches!(&page, Ok(pg) if pg.entries.is_empty() && pg.fragments.is_empty() && !pg.more) {
                    continue;
                }
                print_page(&format!("probe node {n} shard {i}"), &page, 4);
            }
        }
    }
}
