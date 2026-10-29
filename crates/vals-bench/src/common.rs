use super::*;

pub(super) const BLOCK_SIZE: usize = 4096;
pub(super) const SEG_BLOCKS: u64 = 256;

pub(super) fn malloc_trim() {
    #[cfg(target_env = "gnu")]
    unsafe {
        libc::malloc_trim(0);
    }
}

pub(super) fn bench_config(ordering: Ordering) -> Config {
    Config {
        seg_blocks: SEG_BLOCKS,
        reserved_blocks: 0,
        ordering,
    }
}

pub(super) struct Rng(pub(super) u64);
impl Rng {
    pub(super) fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    pub(super) fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

pub(super) fn percentile(sorted_ns: &[u64], p: f64) -> u64 {
    if sorted_ns.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0) * (sorted_ns.len() - 1) as f64).round() as usize;
    sorted_ns[idx]
}

pub(super) fn report(label: &str, mut lat_ns: Vec<u64>, total_secs: f64, bytes: u64) {
    let n = lat_ns.len();
    lat_ns.sort_unstable();
    let ops_per_sec = n as f64 / total_secs;
    println!("--- {label} ---");
    println!("  ops          : {n}");
    println!("  wall         : {total_secs:.3} s");
    println!("  throughput   : {:.0} ops/s", ops_per_sec);
    if bytes > 0 {
        println!(
            "  bandwidth    : {:.1} MiB/s",
            bytes as f64 / total_secs / (1 << 20) as f64
        );
    }
    println!("  latency p50  : {} ns", percentile(&lat_ns, 50.0));
    println!("  latency p99  : {} ns", percentile(&lat_ns, 99.0));
    println!("  latency p999 : {} ns", percentile(&lat_ns, 99.9));
    println!(
        "  latency max  : {} ns",
        lat_ns.last().copied().unwrap_or(0)
    );
}

pub(super) fn hedge_delay_env() -> Option<Duration> {
    match std::env::var("VALS_HEDGE_US").ok()?.trim().parse::<u64>() {
        Ok(us) if us > 0 => Some(Duration::from_micros(us)),
        _ => None,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum SlowMode {
    Digest,
    All,
}

pub(super) fn slow_node_env() -> Option<(u32, Duration, SlowMode)> {
    let raw = std::env::var("VALS_SLOW_NODE").ok()?;
    let mut it = raw.split(':');
    let node: u32 = it.next()?.trim().parse().ok()?;
    let delay_us: u64 = it.next()?.trim().parse().ok()?;
    let mode = match it.next().map(|s| s.trim()) {
        Some("all") => SlowMode::All,
        _ => SlowMode::Digest,
    };
    Some((node, Duration::from_micros(delay_us), mode))
}

async fn slow_delay(d: Duration) {
    if let Ok(timer) = vals_cluster::reactor::Timer::every(d) {
        timer.tick().await;
    }
}

pub(super) struct SlowNode<T> {
    inner: T,
    target: u32,
    delay: Duration,
    mode: SlowMode,
}

impl<T> SlowNode<T> {
    pub(super) fn new(inner: T, target: u32, delay: Duration, mode: SlowMode) -> Self {
        SlowNode {
            inner,
            target,
            delay,
            mode,
        }
    }
}

impl<T: vals_cluster::coord::Transport> vals_cluster::coord::Transport for SlowNode<T> {
    fn set_route_epoch(&self, epoch: u64) {
        self.inner.set_route_epoch(epoch);
    }

    fn set_sorted_ranges(
        &self,
        ranges: &std::collections::HashMap<vals_cluster::ZoneId, vals_cluster::RangeMap>,
    ) {
        self.inner.set_sorted_ranges(ranges);
    }

    fn add_peer(&self, node: vals_cluster::NodeId, addrs: &[std::net::SocketAddr]) {
        self.inner.add_peer(node, addrs);
    }

    fn flush_requests(&self) {
        self.inner.flush_requests();
    }

    fn apply(
        &self,
        node: vals_cluster::NodeId,
        zone_id: vals_cluster::ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        version: vals_cluster::Version,
    ) -> impl std::future::Future<
        Output = Result<vals_cluster::Applied, vals_cluster::coord::TransportError>,
    > {
        self.inner.apply(node, zone_id, key, value, version)
    }

    async fn read(
        &self,
        node: vals_cluster::NodeId,
        zone_id: vals_cluster::ZoneId,
        key: &[u8],
    ) -> Result<Option<vals_cluster::Stored>, vals_cluster::coord::TransportError> {
        if node == self.target && self.mode == SlowMode::All {
            slow_delay(self.delay).await;
        }
        self.inner.read(node, zone_id, key).await
    }

    async fn digest(
        &self,
        node: vals_cluster::NodeId,
        zone_id: vals_cluster::ZoneId,
        key: &[u8],
    ) -> Result<Option<vals_cluster::Version>, vals_cluster::coord::TransportError> {
        if node == self.target {
            slow_delay(self.delay).await;
        }
        self.inner.digest(node, zone_id, key).await
    }

    async fn read_values(
        &self,
        node: vals_cluster::NodeId,
        zone_id: vals_cluster::ZoneId,
        keys: &[Vec<u8>],
    ) -> Vec<Result<Option<vals_cluster::Stored>, vals_cluster::coord::TransportError>> {
        if node == self.target && self.mode == SlowMode::All {
            slow_delay(self.delay).await;
        }
        self.inner.read_values(node, zone_id, keys).await
    }

    fn scan(
        &self,
        node: vals_cluster::NodeId,
        zone_id: vals_cluster::ZoneId,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max: u32,
    ) -> impl std::future::Future<
        Output = Result<vals_cluster::coord::ScanPage, vals_cluster::coord::TransportError>,
    > {
        self.inner.scan(node, zone_id, lo, hi, max)
    }

    fn members(
        &self,
        node: vals_cluster::NodeId,
    ) -> impl std::future::Future<
        Output = Result<vals_cluster::coord::RingView, vals_cluster::coord::TransportError>,
    > {
        self.inner.members(node)
    }

    fn zones(
        &self,
        node: vals_cluster::NodeId,
    ) -> impl std::future::Future<
        Output = Result<vals_cluster::Catalog, vals_cluster::coord::TransportError>,
    > {
        self.inner.zones(node)
    }

    fn range_maps(
        &self,
        node: vals_cluster::NodeId,
    ) -> impl std::future::Future<
        Output = Result<
            Vec<(vals_cluster::ZoneId, vals_cluster::RangeMap)>,
            vals_cluster::coord::TransportError,
        >,
    > {
        self.inner.range_maps(node)
    }

    fn apply_batch(
        &self,
        node: vals_cluster::NodeId,
        zone_id: vals_cluster::ZoneId,
        sync: bool,
        writes: &[vals_cluster::coord::BatchEntry<'_>],
    ) -> impl std::future::Future<Output = Vec<vals_cluster::coord::BatchAck>> {
        self.inner.apply_batch(node, zone_id, sync, writes)
    }
}

pub(super) fn report_tail(label: &str, mut lat_ns: Vec<u64>, secs: f64, errs: usize, hedges: u64) {
    let n = lat_ns.len();
    lat_ns.sort_unstable();
    println!("--- {label} ---");
    println!(
        "  ops={n} errors={errs} wall={secs:.3}s throughput={:.0} ops/s",
        n as f64 / secs
    );
    println!(
        "  latency p50={} p90={} p99={} p99.9={} max={} ns",
        percentile(&lat_ns, 50.0),
        percentile(&lat_ns, 90.0),
        percentile(&lat_ns, 99.0),
        percentile(&lat_ns, 99.9),
        lat_ns.last().copied().unwrap_or(0),
    );
    println!("  digest_hedges_fired (delta this pass) = {hedges}");
}

pub(super) fn make_value(vsize: usize, seed: u64) -> Vec<u8> {
    let mut v = vec![0u8; vsize];
    for (i, b) in v.iter_mut().enumerate() {
        *b = (seed.wrapping_add(i as u64) & 0xFF) as u8;
    }
    v
}

pub(super) fn device_blocks(n: usize, vsize: usize) -> u64 {
    let data = (n as u64) * (vsize as u64 + 64);
    let bytes = (data * 3).max(64 << 20);
    bytes.div_ceil(BLOCK_SIZE as u64)
}

pub(super) fn open_engine(backend: &str, path: &str, n: usize, vsize: usize) -> Engine<Box<dyn BlockDevice>> {
    let blocks = device_blocks(n, vsize);
    let dev: Box<dyn BlockDevice> = match backend {
        "mem" => Box::new(MemBlockDevice::new(BLOCK_SIZE, blocks)),
        "file" => Box::new(FileBlockDevice::create(path, BLOCK_SIZE, blocks).expect("create file")),
        #[cfg(feature = "spdk")]
        "spdk" => {
            let cfg = vals_spdk::spdk::SpdkConfig {
                pci_addr: path.to_string(),
                nsid: 1,
            };
            Box::new(vals_spdk::spdk::SpdkBlockDevice::open(&cfg).expect("open spdk nvme"))
        }
        other => {
            eprintln!(
                "unknown backend '{other}' (expected mem | file | spdk; \
                 spdk needs --features spdk + a PCIe addr as the path arg)"
            );
            std::process::exit(2);
        }
    };
    let cfg = bench_config(Ordering::Unsorted);
    Engine::create(dev, cfg).unwrap()
}

