use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use vals_cluster::{Coordinator, NetTransport, WriteConcern, ZoneId};

type Page = Vec<(Vec<u8>, Vec<u8>)>;
const OPS: [&str; 10] = ["get_vertex", "edge_exists", "out_neighbours", "in_neighbours", "latest_n", "degree", "two_hop", "add_edge", "delete_edge", "delete_vertex"];
const SCAN_METRICS: [&str; 8] = ["scan_run_cursors_total", "scan_run_blocks_total", "scan_run_read_bytes_total", "scan_value_records_total", "scan_value_spans_total", "scan_entries_returned", "scan_sync_fallbacks", "scan_finalize_patches"];

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
    fn pick(&mut self, n: usize) -> usize { (self.next() % n as u64) as usize }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Edge { src: u64, dst: u64, ts: u64 }

fn prefix(id: u64, kind: u8) -> Vec<u8> {
    let mut k = id.to_be_bytes().to_vec();
    k.extend_from_slice(&[0, kind, 0]);
    k
}
fn vertex(id: u64) -> Vec<u8> {
    let mut k = b"v/".to_vec();
    k.extend_from_slice(&id.to_be_bytes());
    k
}
fn edge_key(src: u64, dst: u64) -> Vec<u8> {
    let mut k = prefix(src, b'f');
    k.extend_from_slice(&dst.to_be_bytes());
    k
}
fn latest(e: Edge, natural: bool) -> Vec<u8> {
    let mut k = prefix(e.src, b't');
    k.extend_from_slice(&(if natural { e.ts } else { !e.ts }).to_be_bytes());
    k.push(0);
    k.extend_from_slice(&e.dst.to_be_bytes());
    k
}
fn upper(mut p: Vec<u8>) -> Vec<u8> {
    while p.last() == Some(&255) { p.pop(); }
    *p.last_mut().expect("finite prefix successor") += 1;
    p
}
fn decode(k: &[u8], v: &[u8]) -> Edge {
    assert_eq!(k.len(), 19);
    assert_eq!(&k[8..11], &[0, b'f', 0]);
    assert_eq!(v.len(), 32);
    Edge { src: u64::from_be_bytes(k[..8].try_into().unwrap()), dst: u64::from_be_bytes(k[11..].try_into().unwrap()), ts: u64::from_be_bytes(v[..8].try_into().unwrap()) }
}
fn properties(e: Edge) -> Vec<u8> {
    let mut v = e.ts.to_be_bytes().to_vec();
    v.extend_from_slice(&e.src.to_be_bytes());
    v.extend_from_slice(&e.dst.to_be_bytes());
    v.extend_from_slice(b"follow!!");
    v
}

fn generate(n: usize, mean: usize, seed: u64) -> Vec<Edge> {
    assert!(n >= 16 && mean >= 3 && mean < n / 2);
    let mut rng = Rng(seed);
    let mut cdf = Vec::with_capacity(n);
    let mut sum = 0.0;
    for i in 1..=n { sum += (i as f64).powf(-0.8); cdf.push(sum); }
    let mut next = vec![0usize; n];
    let mut edges = Vec::with_capacity(n.checked_mul(mean).unwrap());
    for src in 0..n {
        for dst in 0..3 {
            if src != dst { edges.push(Edge { src: src as u64, dst: dst as u64, ts: edges.len() as u64 + 1 }); }
        }
    }
    while edges.len() < n * mean {
        let x = (rng.next() >> 11) as f64 / (1u64 << 53) as f64 * sum;
        let src = cdf.partition_point(|&v| v <= x).min(n - 1);
        if next[src] >= n - 3 { continue; }
        let dst = 3 + ((src.wrapping_mul(7919) + next[src]) % (n - 3));
        next[src] += 1;
        if src == dst { continue; }
        edges.push(Edge { src: src as u64, dst: dst as u64, ts: edges.len() as u64 + 1 });
    }
    for i in (1..edges.len()).rev() { let j = rng.pick(i + 1); edges.swap(i, j); }
    edges
}

struct Graph {
    coord: Coordinator<NetTransport>,
    zones: [ZoneId; 3],
    natural: bool,
    scans: std::cell::Cell<u64>,
    returned: std::cell::Cell<u64>,
}
impl Graph {
    fn run<F: std::future::Future>(&self, f: F) -> F::Output {
        vals_rt::block_on(f, vals_cluster::reactor::poll_io)
    }
    fn scan(&self, zone: usize, lo: Option<&[u8]>, hi: Option<&[u8]>, limit: usize) -> Page {
        self.scans.set(self.scans.get() + 1);
        let page = self.run(self.coord.scan_in(self.zones[zone], lo, hi, 2, Some(limit))).expect("graph scan");
        self.returned.set(self.returned.get() + page.iter().map(|(k,v)| (k.len()+v.len()) as u64).sum::<u64>());
        page
    }
    fn neighbours(&self, zone: usize, id: u64, kind: u8, limit: usize) -> Page {
        let lo = prefix(id, kind);
        self.scan(zone, Some(&lo), Some(&upper(lo.clone())), limit)
    }
    fn latest_key(&self, e: Edge) -> Vec<u8> { latest(e, self.natural) }
    fn latest_n(&self, id: u64, limit: usize) -> Page {
        if !self.natural { return self.neighbours(1,id,b't',limit); }
        let lo=prefix(id,b't'); let hi=upper(lo.clone());
        self.scans.set(self.scans.get()+1);
        let page=self.run(self.coord.scan_in_rev(self.zones[1],Some(&lo),Some(&hi),2,Some(limit))).expect("graph reverse scan");
        self.returned.set(self.returned.get()+page.iter().map(|(k,v)|(k.len()+v.len()) as u64).sum::<u64>());
        page
    }
    fn check_latest(&self, n: usize, limit: usize) {
        for id in 0..n.min(100) as u64 {
            let mut edges: Vec<_>=self.neighbours(1,id,b'f',usize::MAX).iter().map(|(k,v)|decode(k,v)).collect();
            edges.sort_unstable_by_key(|e|std::cmp::Reverse(e.ts));
            let expected: Page=edges.into_iter().take(limit).map(|e|(self.latest_key(e),properties(e))).collect();
            assert_eq!(self.latest_n(id,limit),expected,"latest-N vertex {id}");
        }
        println!("GRAPH_LATEST_CHECK vertices={} limit={limit} mismatches=0",n.min(100));
    }
    fn neighbours_many(&self, ids: &[u64], limit: usize, keys_only: bool) -> Vec<Page> {
        let bounds: Vec<_> = ids.iter().map(|&id| { let lo=prefix(id,b'f'); let hi=upper(lo.clone()); (lo,hi) }).collect();
        let scans: Vec<_> = bounds.iter().map(|(lo,hi)| (Some(lo.as_slice()),Some(hi.as_slice()),Some(limit))).collect();
        self.scans.set(self.scans.get()+ids.len() as u64);
        let pages=self.run(self.coord.scan_many_in(self.zones[1],&scans,2,keys_only)).expect("graph multi scan");
        pages.into_iter().map(|p| p.into_iter().map(|(k,s)| {
            let v=s.value.expect("live key");
            self.returned.set(self.returned.get()+(k.len()+v.len()) as u64);
            (k,v)
        }).collect()).collect()
    }
    fn get(&self, zone: usize, key: &[u8]) -> Option<Vec<u8>> {
        let v = self.run(self.coord.read_in(self.zones[zone], key)).expect("graph read").and_then(|s| s.value);
        self.returned.set(self.returned.get() + v.as_ref().map_or(0, |v| v.len() as u64));
        v
    }
    fn many(&self, zone: usize, keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
        let values: Vec<_> = self.run(self.coord.read_many_in(self.zones[zone], keys)).expect("graph read_many")
            .into_iter().map(|v| v.expect("graph read_many item").and_then(|s| s.value)).collect();
        self.returned.set(self.returned.get() + values.iter().flatten().map(|v| v.len() as u64).sum::<u64>());
        values
    }
    fn batch(&self, zone: usize, items: &Page) {
        let refs: Vec<_> = items.iter().map(|(k,v)| (k.as_slice(),v.as_slice())).collect();
        let result = self.run(self.coord.write_batch_in(self.zones[zone], &refs, WriteConcern::All)).expect("graph write_batch");
        assert!(result.failures().next().is_none(), "graph batch item failed: {:?}", result.failures().next());
    }
    fn put_edges(&self, edges: &[Edge]) {
        let out: Page = edges.iter().flat_map(|&e| [(edge_key(e.src,e.dst),properties(e)), (self.latest_key(e),properties(e))]).collect();
        let incoming = edges.iter().map(|&e| (edge_key(e.dst,e.src),properties(e))).collect();
        self.batch(1, &out);
        self.batch(2, &incoming);
    }
    fn delete(&self, zone: usize, key: &[u8]) {
        self.run(self.coord.delete_in(self.zones[zone], key, WriteConcern::All)).expect("graph delete");
    }
    fn delete_edge(&self, e: Edge) {
        self.delete(1, &edge_key(e.src,e.dst));
        self.delete(1, &self.latest_key(e));
        self.delete(2, &edge_key(e.dst,e.src));
    }
    fn range_delete(&self, zone: usize, lo: &[u8]) {
        self.run(self.coord.delete_range_in(self.zones[zone], Some(lo), Some(&upper(lo.to_vec())), WriteConcern::All)).expect("graph delete range");
    }
    fn delete_vertex(&self, id: u64) -> usize {
        let outgoing = self.neighbours(1,id,b'f',usize::MAX);
        for (k,v) in &outgoing {
            let e = decode(k,v);
            self.delete(2,&edge_key(e.dst,e.src));
        }
        self.range_delete(1,&id.to_be_bytes());
        let incoming = self.neighbours(2,id,b'f',usize::MAX);
        for (k,v) in &incoming {
            let r = decode(k,v);
            self.delete_edge(Edge {src:r.dst,dst:r.src,ts:r.ts});
        }
        self.delete(0,&vertex(id));
        outgoing.len() + incoming.len()
    }
    fn pair_check(&self) -> (u64,u64,u64) {
        let mut counts = [0u64;2];
        let mut mismatches = 0;
        for zone in 1..=2 {
            let mut lo = None;
            loop {
                let page = self.scan(zone,lo.as_deref(),None,1024);
                if page.is_empty() { break; }
                let mut next = page.last().unwrap().0.clone(); next.push(0); lo=Some(next);
                if zone==1 {
                    let timed: Vec<_>=page.iter().filter(|(k,_)|k.get(9)==Some(&b't')).collect();
                    let keys: Vec<_>=timed.iter().map(|(k,v)| {
                        assert_eq!(k.len(),28);
                        let e=Edge {src:u64::from_be_bytes(k[..8].try_into().unwrap()),dst:u64::from_be_bytes(k[20..].try_into().unwrap()),ts:{let ts=u64::from_be_bytes(k[11..19].try_into().unwrap()); if self.natural {ts} else {!ts}}};
                        assert_eq!(*k,self.latest_key(e)); assert_eq!(*v,properties(e));
                        edge_key(e.src,e.dst)
                    }).collect();
                    for ((_,v),twin) in timed.into_iter().zip(self.many(1,&keys)) {
                        mismatches+=u64::from(twin.as_ref()!=Some(v));
                    }
                }
                assert!(page.iter().all(|(k,_)|k.get(9)==Some(&b'f') || (zone==1 && k.get(9)==Some(&b't'))),"unexpected graph key");
                let canonical: Page = page.into_iter().filter(|(k,_)| k.get(9)==Some(&b'f')).collect();
                let twins: Vec<_> = canonical.iter().map(|(k,v)| {let e=decode(k,v); edge_key(e.dst,e.src)}).collect();
                let values = self.many(3-zone,&twins);
                for ((_,v),twin) in canonical.iter().zip(values) {
                    mismatches += u64::from(twin.as_ref()!=Some(v));
                }
                if zone==1 {
                    let time_keys: Vec<_> = canonical.iter().map(|(k,v)|self.latest_key(decode(k,v))).collect();
                    for ((_,v),twin) in canonical.iter().zip(self.many(1,&time_keys)) {
                        mismatches += u64::from(twin.as_ref()!=Some(v));
                    }
                }
                counts[zone-1]+=canonical.len() as u64;
            }
        }
        (counts[0],counts[1],mismatches)
    }
}

#[derive(Default)]
struct Stat { ns: Vec<u64>, rpcs: u64, bytes: u64, scans: u64 }
impl Stat {
    fn json(&mut self, wall: f64) -> String {
        self.ns.sort_unstable();
        let n=self.ns.len();
        let q=|p:usize| self.ns[(n*p).div_ceil(1000).max(1)-1] as f64/1000.0;
        format!("{{\"count\":{n},\"ops_s\":{:.3},\"p50_us\":{:.3},\"p99_us\":{:.3},\"p999_us\":{:.3},\"rpcs_query\":{:.3},\"bytes_query\":{:.3},\"scans_query\":{:.3}}}", n as f64/wall,q(500),q(990),q(999),self.rpcs as f64/n as f64,self.bytes as f64/n as f64,self.scans as f64/n as f64)
    }
}

fn metrics() -> BTreeMap<String,u64> {
    if let Ok(endpoints)=std::env::var("VALS_GRAPH_METRICS") {
        use std::io::{Read,Write};
        let mut sums=BTreeMap::new();
        for endpoint in endpoints.split(',') {
            let addr: SocketAddr=endpoint.parse().expect("metrics ip:port");
            let mut s=std::net::TcpStream::connect_timeout(&addr,Duration::from_secs(5)).expect("metrics connect");
            s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
            s.write_all(b"GET /metrics HTTP/1.0\r\n\r\n").unwrap();
            let mut text=String::new(); s.read_to_string(&mut text).unwrap();
            assert!(text.starts_with("HTTP/1.0 200") || text.starts_with("HTTP/1.1 200"),"metrics HTTP status");
            for line in text.lines() {
                let Some((k,v))=line.split_once(' ') else {continue};
                if let Some(k)=k.strip_prefix("vals_") {
                    if let Ok(v)=v.parse::<u64>() {
                        let key = if k.starts_with("engine_resident_runs{") || k.starts_with("engine_frozen_runs{") {
                            format!("{endpoint}/{}",k.replace('"',""))
                        } else { k.to_owned() };
                        *sums.entry(key).or_default()+=v;
                    }
                }
            }
        }
        for k in SCAN_METRICS { assert!(sums.contains_key(k),"missing server metric {k}; install matching build"); }
        sums
    } else {
        let mut m: BTreeMap<String,u64>=vals_store::attribution::counters().into_iter().map(|(k,_,v)|(k.into(),v)).collect();
        m.extend(vals_cluster::observe::snapshot_named().into_iter().map(|(k,_,v)|(k.into(),v)));
        m.extend(vals_cluster::observe::gauges_named().into_iter().map(|(k,_,v)|(k.into(),v)));
        m
    }
}
fn map_json(m: impl IntoIterator<Item=(String,u64)>) -> String {
    format!("{{{}}}",m.into_iter().map(|(k,v)|format!("\"{k}\":{v}")).collect::<Vec<_>>().join(","))
}

fn short_scan_probe(g: &Graph, edges: &[Edge], n: usize, seed: u64, cap: usize, runs: &impl Fn()->String) {
    let queries=env_u64("VALS_GRAPH_SHORT_SCAN_PROBE",0) as usize;
    if queries==0 { return; }
    assert!(queries>=1000,"short-scan p99 needs at least 1000 samples");
    let mut expected=vec![Vec::new();n];
    for &e in edges {expected[e.src as usize].push(e);}
    for page in &mut expected {page.sort_unstable_by_key(|e|e.dst);page.truncate(cap);}
    let runs_before=runs();
    std::thread::sleep(Duration::from_millis(100));
    let before=metrics();
    let mut rng=Rng(seed^0x483);
    let mut stat=Stat::default();
    let mut entries=0;
    let start=Instant::now();
    for _ in 0..queries {
        let id=rng.pick(n);
        let r0=g.coord.transport().rpc_attempts();
        let b0=g.returned.get();
        let t=Instant::now();
        let page=g.neighbours(1,id as u64,b'f',cap);
        stat.ns.push(t.elapsed().as_nanos() as u64);
        stat.rpcs+=g.coord.transport().rpc_attempts()-r0;
        stat.bytes+=g.returned.get()-b0;
        stat.scans+=1;
        entries+=page.len();
        let want: Page=expected[id].iter().map(|&e|(edge_key(e.src,e.dst),properties(e))).collect();
        assert_eq!(page,want,"short-scan probe vertex {id}");
    }
    let wall=start.elapsed().as_secs_f64();
    std::thread::sleep(Duration::from_millis(100));
    let after=metrics();
    let delta: Vec<_>=SCAN_METRICS.iter().map(|&k|(k.to_owned(),after[k].checked_sub(before[k]).expect("counter reset during probe"))).collect();
    assert_eq!(after["scan_sync_fallbacks"],before["scan_sync_fallbacks"]);
    assert_eq!(after["scan_finalize_patches"],before["scan_finalize_patches"]);
    println!("GRAPH_SHORT_SCAN {{\"queries\":{queries},\"limit\":{cap},\"entries\":{entries},\"runs_before\":{runs_before},\"runs_after\":{},\"latency\":{},\"scan_counters\":{}}}",runs(),stat.json(wall),map_json(delta));
}

fn drive(g: Graph, n: usize, vsize: usize, batch: usize, runs: impl Fn()->String) {
    assert!((100..=300).contains(&vsize) && batch>0);
    let seed=env_u64("VALS_GRAPH_SEED",481);
    let mean=env_u64("VALS_GRAPH_MEAN",16) as usize;
    let queries=env_u64("VALS_GRAPH_QUERIES",2000) as usize;
    let cap=env_u64("VALS_GRAPH_FANOUT",10) as usize;
    assert!(queries>=1000 && cap>0);
    for zone in 0..3 { assert!(g.scan(zone,None,None,1).is_empty(),"graph requires fresh zones"); }
    let edges=generate(n,mean,seed);
    let mut fingerprint=0u64;
    for e in &edges {fingerprint=fingerprint.wrapping_mul(31).wrapping_add(e.src).wrapping_mul(31).wrapping_add(e.dst);}
    println!("GRAPH_LOAD vertices={n} edges={} fingerprint={fingerprint:016x}",edges.len());
    for chunk in (0..n).collect::<Vec<_>>().chunks(batch) {
        g.batch(0,&chunk.iter().map(|&i|(vertex(i as u64),make_value(vsize,i as u64))).collect());
    }
    let load=Instant::now();
    for chunk in edges.chunks(batch) {g.put_edges(chunk);}
    let load_secs=load.elapsed().as_secs_f64();
    println!("GRAPH_AGE ack_edges_s={:.1}",edges.len() as f64/load_secs);
    std::thread::sleep(Duration::from_secs(env_u64("VALS_GRAPH_AGE_SECS",20)));
    short_scan_probe(&g,&edges,n,seed,cap,&runs);
    let run_json=runs();
    println!("GRAPH_QUERY runs={run_json}");
    let multi=env_u64("VALS_GRAPH_MULTI_SCAN",0)!=0;
    let before=metrics();
    let mut rng=Rng(seed^0xabcdef);
    let mut stats: [Stat;10]=std::array::from_fn(|_|Stat::default());
    let mut deleted=BTreeSet::new();
    let mut expected=edges.len() as i64;
    let mut ts=edges.len() as u64;
    let start=Instant::now();
    for i in 0..queries {
        let op=match i%1000 {999=>9, x=>match x%100 {0..=19=>0,20..=34=>1,35..=54=>2,55..=69=>3,70..=79=>4,80..=84=>5,85..=89=>6,90..=94=>7,_=>8}};
        let mut id=rng.pick(n) as u64;
        while deleted.contains(&id) {id=rng.pick(n) as u64;}
        let edge=edges[rng.pick(edges.len())];
        let r0=g.coord.transport().rpc_attempts();
        let b0=g.returned.get(); let s0=g.scans.get(); let t=Instant::now();
        match op {
            0=>{assert_eq!(g.get(0,&vertex(id)),Some(make_value(vsize,id)),"vertex properties");}
            1=>{g.get(1,&edge_key(edge.src,edge.dst));}
            2=>{if multi {g.neighbours_many(&[id],cap,false);} else {g.neighbours(1,id,b'f',cap);}}
            3=>{g.neighbours(2,if i%5==0 {rng.pick(3) as u64} else {id},b'f',cap);}
            4=>{g.latest_n(id,cap);}
            5=>{if multi {g.neighbours_many(&[id],usize::MAX,true);} else {g.neighbours(1,id,b'f',usize::MAX);}}
            6=>{
                let mut frontier=BTreeSet::new();
                let first=if multi {g.neighbours_many(&[id],cap,false).pop().unwrap()} else {g.neighbours(1,id,b'f',cap)};
                let ids=first.iter().map(|(k,v)|decode(k,v).dst).collect::<Vec<_>>();
                let pages=if multi {g.neighbours_many(&ids,cap,false)} else {ids.iter().map(|&id|g.neighbours(1,id,b'f',cap)).collect()};
                for page in pages {
                    for (k,v) in page {frontier.insert(decode(&k,&v).dst);}
                }
                let keys=frontier.into_iter().map(vertex).collect::<Vec<_>>(); g.many(0,&keys);
            }
            7=>{
                let mut dst=rng.pick(n) as u64;
                while dst==id || deleted.contains(&dst) {dst=rng.pick(n) as u64;}
                if let Some(v)=g.get(1,&edge_key(id,dst)) {g.delete(1,&g.latest_key(Edge{src:id,dst,ts:u64::from_be_bytes(v[..8].try_into().unwrap())}));}
                else {expected+=1;}
                ts+=1; g.put_edges(&[Edge{src:id,dst,ts}]);
            }
            8=>{
                if let Some((k,v))=g.neighbours(1,id,b'f',1).first() {g.delete_edge(decode(k,v));expected-=1;}
            }
            9=>{
                if id<3 {id=3;}
                while deleted.contains(&id) {id+=1;}
                assert!(id<n as u64);
                expected-=g.delete_vertex(id) as i64; deleted.insert(id);
            }
            _=>unreachable!(),
        }
        let st=&mut stats[op]; st.ns.push(t.elapsed().as_nanos() as u64);
        st.rpcs+=g.coord.transport().rpc_attempts()-r0; st.bytes+=g.returned.get()-b0; st.scans+=g.scans.get()-s0;
    }
    let wall=start.elapsed().as_secs_f64();
    let after=metrics();
    let delta: Vec<_>=SCAN_METRICS.iter().map(|&k|(k.to_owned(),after[k].checked_sub(before[k]).expect("counter reset during arm"))).collect();
    if multi {
        let before=metrics();
        let ids=(0..100).collect::<Vec<_>>();
        let counts=g.neighbours_many(&ids,usize::MAX,true);
        let after=metrics();
        let reads=after["scan_value_records_total"]-before["scan_value_records_total"];
        println!("GRAPH_KEYS_ONLY degree_queries={} entries={} value_records={reads}",ids.len(),counts.iter().map(Vec::len).sum::<usize>());
        assert_eq!(reads,0,"keys-only degree scans must not read value records");
    }
    g.check_latest(n,cap);
    let latest_order=if g.natural {"natural_timestamp_be_reverse"} else {"inverted_timestamp_be"};
    let pair_start=Instant::now();
    let (out,incoming,mismatches)=g.pair_check();
    let ops_json=OPS.iter().zip(&mut stats).map(|(name,s)|format!("\"{name}\":{}",s.json(wall))).collect::<Vec<_>>().join(",");
    println!("GRAPH_SUMMARY {{\"seed\":{seed},\"vertices\":{n},\"mean_degree\":{mean},\"edges_loaded\":{},\"fingerprint\":\"{fingerprint:016x}\",\"latest_order\":\"{latest_order}\",\"load_seconds\":{load_secs:.3},\"ack_edges_s\":{:.3},\"query_seconds\":{wall:.3},\"runs_at_query\":{run_json},\"ops\":{{{ops_json}}},\"scan_counters\":{},\"client_scans\":{},\"pair_check\":{{\"out\":{out},\"in\":{incoming},\"expected\":{expected},\"mismatches\":{mismatches},\"seconds\":{:.3}}}}}",edges.len(),edges.len() as f64/load_secs,map_json(delta),stats.iter().map(|s|s.scans).sum::<u64>(),pair_start.elapsed().as_secs_f64());
    assert_eq!(mismatches,0,"graph pair mismatch"); assert_eq!(out as i64,expected); assert_eq!(incoming,out);
}

fn connect(nodes: Vec<(u32,Vec<SocketAddr>)>) -> Graph {
    let mut ring=vals_cluster::Ring::new(3);
    ring.set_members_weighted(nodes.iter().map(|(id,_)|(*id,1)));
    let coord=Coordinator::new(7481,ring,2,NetTransport::connect_sharded(nodes).with_rpc_deadline(Duration::from_secs(30)));
    assert!(poll_until(Duration::from_secs(60),Duration::from_millis(200),||vals_rt::block_on(coord.adopt_from_any_member(),vals_cluster::reactor::poll_io)),"adopt graph catalog");
    let names=["gv","ge","gr"].map(|s|std::env::var(format!("VALS_GRAPH_{}",s.to_uppercase())).unwrap_or(s.into()));
    assert!(poll_until(Duration::from_secs(60),Duration::from_millis(200),||{
        vals_rt::block_on(coord.adopt_from_any_member(),vals_cluster::reactor::poll_io);
        names.iter().all(|name|coord.catalog().by_name(name).is_some())
    }),"graph zones committed and pushed");
    let zones=names.map(|name|coord.catalog().by_name(&name).expect("graph zone exists"));
    let g=Graph{coord,zones,natural:env_u64("VALS_GRAPH_REVERSE",0)!=0,scans:std::cell::Cell::new(0),returned:std::cell::Cell::new(0)};
    for zone in zones {
        assert!(poll_until(Duration::from_secs(60),Duration::from_millis(200),||g.run(g.coord.scan_in(zone,None,None,2,Some(1))).is_ok()),"graph zone ready");
    }
    g
}

pub(super) fn network(spec: &str,n: usize,vsize: usize,batch: usize) {
    assert!(std::env::var("VALS_GRAPH_METRICS").is_ok(),"netgraph requires VALS_GRAPH_METRICS endpoints");
    let nodes=spec.split(';').map(|group|{
        let (id,addrs)=group.split_once('@').expect("id@ip:port,ip:port;...");
        (id.parse().unwrap(),addrs.split(',').map(|s|s.parse().unwrap()).collect())
    }).collect();
    let g=connect(nodes);
    let zones=g.zones;
    drive(g,n,vsize,batch,||{
        let m=metrics();
        let runs: Vec<_>=m.into_iter().filter(|(k,_)| {
            k.contains("/engine_") && zones.iter().any(|z|k.ends_with(&format!("/{z}}}")))
        }).collect();
        assert!(!runs.is_empty(),"missing per-engine run metrics for graph zones");
        map_json(runs)
    });
}

pub(super) fn local(backend: &str,dir: &str,n: usize,vsize: usize,batch: usize) {
    use vals_cluster::liveness::MemLivenessLog;
    use vals_cluster::pd::{PdDaemon,RingConfig};
    use vals_cluster::zonestore::{FileZoneStore,MemZoneStore,ZoneGeom,ZoneStore};
    use vals_cluster::{Catalog,Node,Ordering as ZOrdering,Ring,ZoneConfig,DEFAULT_ZONE};
    let settings=node_settings();
    let geom=ZoneGeom{block_size:BLOCK_SIZE,seg_blocks:SEG_BLOCKS,reserved_blocks:0,region_blocks:env_u64("VALS_GRAPH_REGION_MIB",2048)*1024*1024/BLOCK_SIZE as u64};
    let mut built=Vec::new();
    for id in 1..=3 {let (listener,addr)=Node::bind().unwrap();built.push((id,listener,addr));}
    let nodes: Vec<_>=built.iter().map(|(id,_,addr)|(*id,vec![*addr])).collect();
    let mut ring=Ring::new(3);ring.set_members_weighted(nodes.iter().map(|(id,_)|(*id,1)));
    let mut handles=Vec::new();
    for (id,listener,_) in built {
        let peers=nodes.iter().filter(|(i,_)|*i!=id).map(|(i,a)|(*i,a[0])).collect();
        let sweep=Duration::from_secs(5);
        let h=match backend {
            "file"=>{
                let p=FileZoneStore::new(id,format!("{dir}/node{id}"),geom).unwrap();let z=p.open(DEFAULT_ZONE,ZOrdering::Unsorted).unwrap();
                Node::serve_zoned_with_liveness_and_settings(id,listener,z,p,Catalog::new(),ring.clone(),peers,sweep,60_000,MemLivenessLog::new(),None,settings.clone())
            }
            "mem"=>{
                let p=MemZoneStore::new(id,geom);let z=p.open(DEFAULT_ZONE,ZOrdering::Unsorted).unwrap();
                Node::serve_zoned_with_liveness_and_settings(id,listener,z,p,Catalog::new(),ring.clone(),peers,sweep,60_000,MemLivenessLog::new(),None,settings.clone())
            }
            _=>panic!("graph supports mem/file; use netgraph for SPDK"),
        };handles.push(h);
    }
    let engine=Engine::create_prewiped(MemBlockDevice::new(4096,16*1024),Config::default());
    let pd=PdDaemon::spawn_single(9481,engine,RingConfig{vnodes:vals_cluster::ring::DEFAULT_VNODES,rf:3},nodes.clone()).unwrap();
    if !poll_until(Duration::from_secs(2),Duration::from_millis(20),||!pd.members().is_empty()) {pd.bootstrap_members(nodes.iter().map(|(id,a)|(*id,1,a.clone())).collect());}
    for name in ["gv","ge","gr"] {pd.create_zone(ZoneConfig{name:name.into(),rf:3,ordering:ZOrdering::Sorted,scope:None}).unwrap();}
    assert!(poll_until(Duration::from_secs(30),Duration::from_millis(100),||pd.catalog().by_name("gr").is_some()));
    drive(connect(nodes),n,vsize,batch,||{
        let mut entries=Vec::new();
        for (i,h) in handles.iter().enumerate() {
            for (_,s) in h.dumper().all_zone_stats().unwrap() {
                if s.zone_id!=DEFAULT_ZONE {entries.push((format!("node{}_zone{}",i+1,s.zone_id),s.resident_runs as u64));}
            }
        }
        entries.push(("frozen_runs_total".into(),vals_cluster::observe::gauges().frozen_runs.get()));
        map_json(entries)
    });
    for h in handles {h.shutdown();} pd.shutdown();
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn seeded_graph_is_unique_skewed_and_has_celebrities() {
        let edges=generate(1000,16,481);
        assert_eq!(edges,generate(1000,16,481)); assert_ne!(edges,generate(1000,16,482));
        assert_eq!(edges.len(),16000);
        let pairs: BTreeSet<_>=edges.iter().map(|e|(e.src,e.dst)).collect(); assert_eq!(pairs.len(),edges.len());
        assert!(edges.iter().all(|e|e.src!=e.dst && e.src<1000 && e.dst<1000));
        for dst in 0..3 {assert_eq!(edges.iter().filter(|e|e.dst==dst).count(),999);}
        let mut degrees=vec![0;1000]; for e in edges {degrees[e.src as usize]+=1;}
        assert!(degrees[0]>100); assert!(degrees[999]<20);
    }
    #[test]
    fn fixed_width_bounds_and_latest_order() {
        let p=prefix(17,b'f');let hi=upper(p.clone());
        for id in [0,255,u64::MAX] {let k=edge_key(17,id);assert!(p<=k && k<hi);}
        assert!(edge_key(18,0)>=hi);assert!(edge_key(16,u64::MAX)<p);
        let a=Edge{src:17,dst:4,ts:42};let b=Edge{ts:43,..a};assert!(latest(b,false)<latest(a,false));
        assert!(latest(b,true)>latest(a,true));
        for natural in [false,true] {
            let p=prefix(a.src,b't');
            for ts in [0,42,u64::MAX] {let k=latest(Edge{ts,..a},natural); assert!(p<=k && k<upper(p.clone()));}
        }
        assert_eq!(decode(&edge_key(a.src,a.dst),&properties(a)),a);
    }
}
