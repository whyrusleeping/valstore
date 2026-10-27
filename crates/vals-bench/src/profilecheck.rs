use super::*;

const PROFILE_STATE_MAGIC: &[u8; 8] = b"VPCSTATE";
const PROFILE_STATE_VERSION: u64 = 4;
const PROFILE_SEED_IDENTITY: u64 = 0x0194_5eed_dead_beef;
const PROFILE_COUNTER_BITS: u32 = 16;
const PROFILE_WRITERS: [u64; 3] = [
    0x0194_0001_0000_0001,
    0x0194_0002_0000_0002,
    0x0194_0003_0000_0003,
];

type Reference = std::collections::BTreeMap<Vec<u8>, (vals_cluster::Version, Option<Vec<u8>>)>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Model {
    seed_keys: usize,
    workload_keys: usize,
    gc_grace_ms: u64,
    skew_margin_ms: u64,
}

impl Model {
    fn max_keys(&self) -> usize {
        self.seed_keys.saturating_add(self.workload_keys.saturating_mul(5))
    }

    fn slot(&self, round: usize) -> usize {
        if self.workload_keys == 0 { 0 } else { round % self.workload_keys }
    }

    fn cycle(&self, round: usize) -> usize {
        if self.workload_keys == 0 { round } else { round / self.workload_keys }
    }

    fn winner_is_tombstone(&self, round: usize, domain: usize) -> bool {
        (self.cycle(round) + self.slot(round) + domain) % 4 == 1
    }

    fn tombstone_must_be_present(&self, version: vals_cluster::Version, now_ms: u64) -> bool {
        let age = now_ms.saturating_sub(version.physical_ms());
        age.saturating_add(self.skew_margin_ms) <= self.gc_grace_ms
    }

    fn tombstone_is_overdue(&self, version: vals_cluster::Version, now_ms: u64, overdue_ms: u64) -> bool {
        let age = now_ms.saturating_sub(version.physical_ms());
        age >= self.gc_grace_ms.saturating_add(self.skew_margin_ms).saturating_add(overdue_ms)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct GcProbe {
    collected: u64,
    held: u64,
    stale: u64,
    unreadable: u64,
}

impl GcProbe {
    fn verdict(&self, max_held_pct: u64) -> Result<(), String> {
        let read = self.collected + self.held;
        if read == 0 {
            return Ok(());
        }
        if self.collected == 0 {
            return Err(format!("collected none of {read} overdue winning tombstones"));
        }
        if self.held.saturating_mul(100) > read.saturating_mul(max_held_pct) {
            return Err(format!(
                "still holds {} of {read} overdue winning tombstones, above the {max_held_pct}% bound",
                self.held
            ));
        }
        Ok(())
    }
}

struct RoundPlan {
    direct_key: Vec<u8>,
    direct_values: [Option<Vec<u8>>; 3],
    batch_key: Vec<u8>,
    batch_values: [Option<Vec<u8>>; 3],
    coordinator_key: Vec<u8>,
    coordinator_value: Vec<u8>,
    cas_key: Vec<u8>,
    retired_key: Option<Vec<u8>>,
    seed_overwrite: Option<(Vec<u8>, Vec<u8>)>,
}

fn seed_key(index: usize) -> Vec<u8> {
    format!("seed-{index:08}").into_bytes()
}

fn round_plan(round: usize, model: &Model) -> RoundPlan {
    let slot = model.slot(round);
    let direct_tombstone = model.winner_is_tombstone(round, 0);
    let batch_tombstone = model.winner_is_tombstone(round, 1);
    RoundPlan {
        direct_key: format!("profile-{slot:012}").into_bytes(),
        direct_values: [
            Some(format!("direct-old-{round}").into_bytes()),
            if direct_tombstone {
                Some(format!("direct-middle-{round}").into_bytes())
            } else {
                None
            },
            if direct_tombstone {
                None
            } else {
                Some(format!("direct-winner-{round}").into_bytes())
            },
        ],
        batch_key: format!("batch-{slot:012}").into_bytes(),
        batch_values: [
            Some(format!("batch-old-{round}").into_bytes()),
            if batch_tombstone {
                Some(format!("batch-middle-{round}").into_bytes())
            } else {
                None
            },
            if batch_tombstone {
                None
            } else {
                Some(format!("batch-winner-{round}").into_bytes())
            },
        ],
        coordinator_key: format!("coordinator-{slot:012}").into_bytes(),
        coordinator_value: format!("coordinator-value-{round}").into_bytes(),
        cas_key: format!("cas-{slot:012}").into_bytes(),
        retired_key: (model.cycle(round) == 0).then(|| format!("retired-{slot:012}").into_bytes()),
        seed_overwrite: (model.seed_keys > 0).then(|| {
            let index = round.wrapping_mul(2_654_435_761) % model.seed_keys;
            (seed_key(index), format!("seed-rewrite-{round}").into_bytes())
        }),
    }
}

#[derive(Default)]
struct VersionSource {
    last_hlc: u64,
}

impl VersionSource {
    fn resumed_from(high_hlc: u64, reference: &Reference) -> Self {
        let highest = reference.values().map(|(version, _)| version.hlc).max().unwrap_or(0);
        VersionSource { last_hlc: high_hlc.max(highest) }
    }

    fn observe_at_least(&mut self, hlc: u64) {
        if hlc > self.last_hlc {
            self.last_hlc = hlc;
        }
    }

    fn next_triple(&mut self, physical_ms: u64) -> [vals_cluster::Version; 3] {
        let mut out = [vals_cluster::Version { hlc: 0, node: 0 }; 3];
        for (i, slot) in out.iter_mut().enumerate() {
            let physical = physical_ms << PROFILE_COUNTER_BITS;
            let hlc = if physical > self.last_hlc { physical } else { self.last_hlc + 1 };
            self.last_hlc = hlc;
            *slot = vals_cluster::Version { hlc, node: PROFILE_WRITERS[i] };
        }
        out
    }

    fn high_hlc(&self) -> u64 {
        self.last_hlc
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Api {
    PointRead,
    Replica,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
struct Counts {
    live: u64,
    tombstones: u64,
    collected: u64,
    hidden: u64,
}

#[derive(Debug)]
enum Divergence {
    Soft(String),
    Hard(String),
}

fn scan_successor(mut key: Vec<u8>) -> Vec<u8> {
    key.push(0);
    key
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn dirty_resume_requested(value: Option<&std::ffi::OsStr>) -> bool {
    value == Some(std::ffi::OsStr::new("1"))
}

fn reconciliation_is_allowed(
    state_was_loaded: bool,
    state_clean: bool,
    value: Option<&std::ffi::OsStr>,
) -> bool {
    state_was_loaded && !state_clean && dirty_resume_requested(value)
}

fn profile_physical_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn process_rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1).and_then(|v| v.parse().ok()))
        })
        .unwrap_or(0)
}

fn state_u64(bytes: &[u8], at: &mut usize) -> Result<u64, String> {
    let end = at.checked_add(8).ok_or("profilecheck state offset overflow")?;
    let raw = bytes.get(*at..end).ok_or("truncated profilecheck state")?;
    *at = end;
    Ok(u64::from_le_bytes(raw.try_into().unwrap()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
struct StateHeader {
    round: usize,
    clean: bool,
    high_hlc: u64,
}

fn decode_state(
    bytes: &[u8],
    expected_zone: vals_cluster::ZoneId,
    expected: &Model,
) -> Result<(StateHeader, Reference), String> {
    if bytes.get(..PROFILE_STATE_MAGIC.len()) != Some(PROFILE_STATE_MAGIC) {
        return Err("incompatible profilecheck state magic (refusing legacy/unversioned state)".into());
    }
    let mut at = PROFILE_STATE_MAGIC.len();
    let format = state_u64(bytes, &mut at)?;
    if format != PROFILE_STATE_VERSION {
        return Err(format!("unsupported profilecheck state version {format}"));
    }
    let zone = state_u64(bytes, &mut at)?;
    if zone != expected_zone as u64 {
        return Err(format!("profilecheck state zone {zone} does not match requested zone {expected_zone}"));
    }
    let seed_identity = state_u64(bytes, &mut at)?;
    if seed_identity != PROFILE_SEED_IDENTITY {
        return Err("profilecheck state seed identity mismatch".into());
    }
    let seed_keys = state_u64(bytes, &mut at)?;
    if seed_keys != expected.seed_keys as u64 {
        return Err(format!("profilecheck state seed_keys {seed_keys} does not match requested {}", expected.seed_keys));
    }
    let workload_keys = state_u64(bytes, &mut at)?;
    if workload_keys != expected.workload_keys as u64 {
        return Err(format!("profilecheck state workload_keys {workload_keys} does not match requested {}", expected.workload_keys));
    }
    let gc_grace_ms = state_u64(bytes, &mut at)?;
    if gc_grace_ms != expected.gc_grace_ms {
        return Err(format!("profilecheck state gc_grace_ms {gc_grace_ms} does not match requested {}", expected.gc_grace_ms));
    }
    let round_u64 = state_u64(bytes, &mut at)?;
    let round = usize::try_from(round_u64).map_err(|_| "profilecheck round does not fit usize")?;
    let clean = match state_u64(bytes, &mut at)? {
        0 => false,
        1 => true,
        other => return Err(format!("profilecheck state clean flag {other} is not 0 or 1")),
    };
    let high_hlc = state_u64(bytes, &mut at)?;
    let count = state_u64(bytes, &mut at)?;
    let count = usize::try_from(count).map_err(|_| "profilecheck entry count does not fit usize")?;
    let mut reference = Reference::new();
    for _ in 0..count {
        let klen = usize::try_from(state_u64(bytes, &mut at)?)
            .map_err(|_| "profilecheck key length does not fit usize")?;
        let end = at.checked_add(klen).ok_or("profilecheck key offset overflow")?;
        let key = bytes.get(at..end).ok_or("truncated profilecheck state key")?.to_vec();
        at = end;
        let version = vals_cluster::Version {
            hlc: state_u64(bytes, &mut at)?,
            node: state_u64(bytes, &mut at)?,
        };
        let vlen = state_u64(bytes, &mut at)?;
        let value = if vlen == u64::MAX {
            None
        } else {
            let n = usize::try_from(vlen).map_err(|_| "profilecheck value length does not fit usize")?;
            let end = at.checked_add(n).ok_or("profilecheck value offset overflow")?;
            let value = bytes.get(at..end).ok_or("truncated profilecheck state value")?.to_vec();
            at = end;
            Some(value)
        };
        if reference.insert(key, (version, value)).is_some() {
            return Err("duplicate key in profilecheck state".into());
        }
    }
    if at != bytes.len() {
        return Err("trailing profilecheck state bytes".into());
    }
    if reference.len() > expected.max_keys() {
        return Err(format!(
            "profilecheck state holds {} keys above the bounded model maximum {}",
            reference.len(),
            expected.max_keys()
        ));
    }
    let highest = reference.values().map(|(version, _)| version.hlc).max().unwrap_or(0);
    if high_hlc < highest {
        return Err(format!(
            "profilecheck state high_hlc {high_hlc} is below its own highest reference version {highest}"
        ));
    }
    Ok((StateHeader { round, clean, high_hlc }, reference))
}

fn encode_state(
    header: StateHeader,
    zone: vals_cluster::ZoneId,
    model: &Model,
    reference: &Reference,
) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(PROFILE_STATE_MAGIC);
    for n in [
        PROFILE_STATE_VERSION,
        zone as u64,
        PROFILE_SEED_IDENTITY,
        model.seed_keys as u64,
        model.workload_keys as u64,
        model.gc_grace_ms,
        header.round as u64,
        u64::from(header.clean),
        header.high_hlc,
        reference.len() as u64,
    ] {
        bytes.extend_from_slice(&n.to_le_bytes());
    }
    for (key, (version, value)) in reference {
        bytes.extend_from_slice(&(key.len() as u64).to_le_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(&version.hlc.to_le_bytes());
        bytes.extend_from_slice(&version.node.to_le_bytes());
        match value {
            Some(value) => {
                bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
                bytes.extend_from_slice(value);
            }
            None => bytes.extend_from_slice(&u64::MAX.to_le_bytes()),
        }
    }
    bytes
}

fn state_header(round: usize, clean: bool, source: &VersionSource, reference: &Reference) -> StateHeader {
    let highest = reference.values().map(|(version, _)| version.hlc).max().unwrap_or(0);
    StateHeader { round, clean, high_hlc: source.high_hlc().max(highest) }
}

fn save_state(
    path: &std::path::Path,
    header: StateHeader,
    zone: vals_cluster::ZoneId,
    model: &Model,
    reference: &Reference,
) {
    use std::io::Write as _;

    let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
    let mut file = std::fs::File::create(&tmp).expect("create profilecheck state temporary file");
    file.write_all(&encode_state(header, zone, model, reference))
        .expect("write profilecheck state");
    file.sync_all().expect("sync profilecheck state");
    std::fs::rename(&tmp, path).expect("atomically replace profilecheck state");
    if let Some(parent) = path.parent() {
        std::fs::File::open(parent).and_then(|dir| dir.sync_all())
            .expect("sync profilecheck state directory");
    }
}

async fn profile_retry_delay() {
    if let Ok(timer) = vals_cluster::reactor::Timer::every(Duration::from_millis(25)) {
        timer.tick().await;
    } else {
        vals_rt::yield_now().await;
    }
}

async fn profile_sleep(period: Duration) {
    if let Ok(timer) = vals_cluster::reactor::Timer::every(period) {
        timer.tick().await;
    } else {
        vals_rt::yield_now().await;
    }
}

thread_local! {
    static DIRECT_RANGES: std::cell::RefCell<std::collections::HashMap<vals_cluster::ZoneId, vals_cluster::RangeMap>> =
        std::cell::RefCell::new(std::collections::HashMap::new());
}

async fn refresh_direct_routes(transport: &vals_cluster::NetTransport, node: u32, epoch: u64) {
    vals_cluster::Transport::set_route_epoch(transport, epoch);
    if let Ok(maps) = vals_cluster::Transport::range_maps(transport, node).await {
        let maps: std::collections::HashMap<_, _> = maps.into_iter().collect();
        vals_cluster::Transport::set_sorted_ranges(transport, &maps);
        DIRECT_RANGES.with(|ranges| *ranges.borrow_mut() = maps);
    }
}

fn direct_slice_hi(zone: vals_cluster::ZoneId, lo: Option<&[u8]>) -> Option<Vec<u8>> {
    DIRECT_RANGES.with(|ranges| {
        ranges.borrow().get(&zone).and_then(|map| map.ranges_overlapping(lo, None).into_iter().next()?.hi)
    })
}

async fn profile_apply_retry(
    transport: &vals_cluster::NetTransport,
    node: u32,
    zone: vals_cluster::ZoneId,
    key: &[u8],
    value: Option<&[u8]>,
    version: vals_cluster::Version,
    retry: Duration,
) -> Result<vals_cluster::Applied, vals_cluster::TransportError> {
    let deadline = Instant::now() + retry;
    loop {
        match vals_cluster::Transport::apply(transport, node, zone, key, value, version).await {
            Ok(applied) => return Ok(applied),
            Err(vals_cluster::TransportError::StaleEpoch(epoch)) if Instant::now() < deadline => {
                refresh_direct_routes(transport, node, epoch).await;
                profile_retry_delay().await;
            }
            Err(e) if Instant::now() < deadline => { let _ = e; profile_retry_delay().await; }
            Err(e) => return Err(e),
        }
    }
}

async fn profile_node_read_retry(
    transport: &vals_cluster::NetTransport,
    node: u32,
    zone: vals_cluster::ZoneId,
    key: &[u8],
    retry: Duration,
) -> Result<Option<vals_cluster::Stored>, vals_cluster::TransportError> {
    let deadline = Instant::now() + retry;
    loop {
        match vals_cluster::Transport::read(transport, node, zone, key).await {
            Ok(stored) => return Ok(stored),
            Err(vals_cluster::TransportError::StaleEpoch(epoch)) if Instant::now() < deadline => {
                refresh_direct_routes(transport, node, epoch).await;
                profile_retry_delay().await;
            }
            Err(e) if Instant::now() < deadline => { let _ = e; profile_retry_delay().await; }
            Err(e) => return Err(e),
        }
    }
}

async fn profile_scan_retry(
    transport: &vals_cluster::NetTransport,
    node: u32,
    zone: vals_cluster::ZoneId,
    lo: Option<&[u8]>,
    retry: Duration,
) -> (vals_cluster::coord::ScanPage, Option<Vec<u8>>) {
    let deadline = Instant::now() + retry;
    loop {
        let hi = direct_slice_hi(zone, lo);
        match vals_cluster::Transport::scan(transport, node, zone, lo, hi.as_deref(), 4096).await {
            Ok(page) => return (page, hi),
            Err(vals_cluster::TransportError::StaleEpoch(epoch)) if Instant::now() < deadline => {
                refresh_direct_routes(transport, node, epoch).await;
                profile_retry_delay().await;
            }
            Err(e) if Instant::now() < deadline => { let _ = e; profile_retry_delay().await; }
            Err(e) => panic!("Scan retry deadline node={node}: {e:?}"),
        }
    }
}

async fn coordinator_read_retry(
    coordinator: &vals_cluster::Coordinator<vals_cluster::NetTransport>,
    zone: vals_cluster::ZoneId,
    key: &[u8],
    retry: Duration,
) -> Option<vals_cluster::Stored> {
    let deadline = Instant::now() + retry;
    loop {
        match coordinator.read_in(zone, key).await {
            Ok(value) => return value,
            Err(e) if Instant::now() < deadline => { let _ = e; profile_retry_delay().await; }
            Err(e) => panic!("Coordinator read retry deadline key={:?}: {e:?}", String::from_utf8_lossy(key)),
        }
    }
}

async fn coordinator_write_retry(
    coordinator: &vals_cluster::Coordinator<vals_cluster::NetTransport>,
    zone: vals_cluster::ZoneId,
    key: &[u8],
    value: &[u8],
    retry: Duration,
) -> vals_cluster::Version {
    use vals_cluster::WriteConcern;

    let deadline = Instant::now() + retry;
    loop {
        match coordinator.write_in(zone, key, value, WriteConcern::Majority).await {
            Ok(version) => return version,
            Err(e) => {
                if let Ok(Some(got)) = coordinator.read_in(zone, key).await {
                    if got.value.as_deref() == Some(value) {
                        return got.version;
                    }
                }
                assert!(Instant::now() < deadline, "Coordinator write retry deadline key={:?}: {e:?}", String::from_utf8_lossy(key));
                profile_retry_delay().await;
            }
        }
    }
}

async fn coordinator_cas_mismatch_retry(
    coordinator: &vals_cluster::Coordinator<vals_cluster::NetTransport>,
    zone: vals_cluster::ZoneId,
    key: &[u8],
    stale_expected: vals_cluster::Version,
    current: vals_cluster::Version,
    current_value: &[u8],
    retry: Duration,
) {
    use vals_cluster::CasResult;

    let deadline = Instant::now() + retry;
    loop {
        match coordinator.cas_in(zone, key, b"must-not-apply", Some(stale_expected)).await {
            Ok(CasResult::Mismatch { current: observed }) => {
                assert_eq!(observed, Some(current));
                return;
            }
            Ok(CasResult::Applied(version)) => panic!("CAS applied despite stale precondition: {version:?}"),
            Err(e) => {
                if let Ok(Some(got)) = coordinator.read_in(zone, key).await {
                    assert_eq!(got.version, current, "failed mismatch-CAS changed current version");
                    assert_eq!(got.value.as_deref(), Some(current_value), "failed mismatch-CAS changed value");
                }
                assert!(Instant::now() < deadline, "CAS mismatch retry deadline key={:?}: {e:?}", String::from_utf8_lossy(key));
                profile_retry_delay().await;
            }
        }
    }
}

enum CasReadback {
    Applied(vals_cluster::Version),
    Retry,
}

fn classify_ambiguous_cas(
    expected: vals_cluster::Version,
    desired: &[u8],
    got: Option<&vals_cluster::Stored>,
) -> Result<CasReadback, String> {
    let got = got.ok_or_else(|| "ambiguous CAS readback found the key absent".to_string())?;
    if got.value.as_deref() == Some(desired) {
        return Ok(CasReadback::Applied(got.version));
    }
    if got.version == expected {
        return Ok(CasReadback::Retry);
    }
    Err(format!("ambiguous CAS readback found unexpected version {:?}", got.version))
}

async fn coordinator_cas_retry(
    coordinator: &vals_cluster::Coordinator<vals_cluster::NetTransport>,
    zone: vals_cluster::ZoneId,
    key: &[u8],
    value: &[u8],
    expected: vals_cluster::Version,
    retry: Duration,
) -> vals_cluster::Version {
    use vals_cluster::CasResult;

    let deadline = Instant::now() + retry;
    loop {
        match coordinator.cas_in(zone, key, value, Some(expected)).await {
            Ok(CasResult::Applied(version)) => return version,
            Ok(CasResult::Mismatch { current }) => {
                let got = coordinator_read_retry(coordinator, zone, key, retry).await;
                if let Some(got) = got {
                    if got.value.as_deref() == Some(value) {
                        return got.version;
                    }
                }
                panic!("CAS precondition changed without requested value key={:?} current={current:?}", String::from_utf8_lossy(key));
            }
            Err(e) => {
                loop {
                    match coordinator.read_in(zone, key).await {
                        Ok(got) => match classify_ambiguous_cas(expected, value, got.as_ref()) {
                            Ok(CasReadback::Applied(version)) => return version,
                            Ok(CasReadback::Retry) => break,
                            Err(reason) => panic!("unsafe ambiguous CAS state key={:?}: {reason}", String::from_utf8_lossy(key)),
                        },
                        Err(read_error) if Instant::now() < deadline => {
                            let _ = read_error;
                            profile_retry_delay().await;
                        }
                        Err(read_error) => panic!("CAS readback retry deadline key={:?}: write={e:?} read={read_error:?}", String::from_utf8_lossy(key)),
                    }
                }
                assert!(Instant::now() < deadline, "CAS retry deadline key={:?}: {e:?}", String::from_utf8_lossy(key));
                profile_retry_delay().await;
            }
        }
    }
}

fn check_expected(
    api: Api,
    key: &[u8],
    expected: &(vals_cluster::Version, Option<Vec<u8>>),
    got: Option<&vals_cluster::Stored>,
    now_ms: u64,
    model: &Model,
) -> Result<Counts, Divergence> {
    let name = String::from_utf8_lossy(key).into_owned();
    let mut counts = Counts::default();
    match (&expected.1, got) {
        (Some(value), Some(got)) => {
            if got.version > expected.0 {
                return Err(Divergence::Hard(format!(
                    "key={name} exposed version {:?} above the highest written version {:?}",
                    got.version, expected.0
                )));
            }
            if got.version < expected.0 {
                let message = format!(
                    "key={name} still holds stale version {:?} below expected {:?}",
                    got.version, expected.0
                );
                return match api {
                    Api::PointRead => Err(Divergence::Hard(message)),
                    Api::Replica => Err(Divergence::Soft(message)),
                };
            }
            if got.value.as_deref() != Some(value.as_slice()) {
                return Err(Divergence::Hard(format!(
                    "key={name} value mismatch at winning version {:?}",
                    expected.0
                )));
            }
            counts.live += 1;
        }
        (Some(_), None) => {
            let message = format!("key={name} is missing its live winner {:?}", expected.0);
            return match api {
                Api::PointRead => Err(Divergence::Hard(message)),
                Api::Replica => Err(Divergence::Soft(message)),
            };
        }
        (None, Some(got)) => {
            if api == Api::PointRead {
                return Err(Divergence::Hard(format!(
                    "key={name} is resurrected by a point read as version {:?}",
                    got.version
                )));
            }
            if got.version > expected.0 {
                return Err(Divergence::Hard(format!(
                    "key={name} exposed version {:?} above the winning tombstone {:?}",
                    got.version, expected.0
                )));
            }
            if got.version < expected.0 {
                return Err(Divergence::Soft(format!(
                    "key={name} still holds pre-tombstone version {:?}",
                    got.version
                )));
            }
            if got.value.is_some() {
                return Err(Divergence::Hard(format!(
                    "key={name} resurrected a value at the winning tombstone version {:?}",
                    expected.0
                )));
            }
            counts.tombstones += 1;
        }
        (None, None) => match api {
            Api::PointRead => counts.hidden += 1,
            Api::Replica => {
                if model.tombstone_must_be_present(expected.0, now_ms) {
                    return Err(Divergence::Soft(format!(
                        "key={name} is missing its winning tombstone {:?} inside gc_grace",
                        expected.0
                    )));
                }
                counts.collected += 1;
            }
        },
    }
    Ok(counts)
}

fn assert_expected(
    api: Api,
    key: &[u8],
    expected: &(vals_cluster::Version, Option<Vec<u8>>),
    got: Option<&vals_cluster::Stored>,
    now_ms: u64,
    model: &Model,
    context: &str,
) -> Result<Counts, String> {
    match check_expected(api, key, expected, got, now_ms, model) {
        Ok(counts) => Ok(counts),
        Err(Divergence::Hard(reason)) => panic!("PROFILECHECK CORRECTNESS {context}: {reason}"),
        Err(Divergence::Soft(reason)) => Err(reason),
    }
}

async fn verify_node_stream(
    transport: &vals_cluster::NetTransport,
    node: u32,
    zone: vals_cluster::ZoneId,
    reference: &Reference,
    model: &Model,
    retry: Duration,
) -> Result<Counts, Divergence> {
    let mut counts = Counts::default();
    let mut expected = reference.iter().peekable();
    let mut cursor: Option<Vec<u8>> = None;
    let mut previous: Option<Vec<u8>> = None;
    let mut soft: Option<String> = None;
    let mut soft_keys = 0u64;
    let fold = |result: Result<Counts, Divergence>,
                    counts: &mut Counts,
                    soft: &mut Option<String>,
                    soft_keys: &mut u64|
     -> Result<(), Divergence> {
        match result {
            Ok(seen) => {
                counts.live += seen.live;
                counts.tombstones += seen.tombstones;
                counts.collected += seen.collected;
                counts.hidden += seen.hidden;
                Ok(())
            }
            Err(Divergence::Hard(reason)) => Err(Divergence::Hard(reason)),
            Err(Divergence::Soft(reason)) => {
                *soft_keys += 1;
                if soft.is_none() {
                    *soft = Some(reason);
                }
                Ok(())
            }
        }
    };
    loop {
        let (page, slice_hi) = profile_scan_retry(transport, node, zone, cursor.as_deref(), retry).await;
        let last = page.entries.last().map(|(key, _)| key.clone());
        let resume = page.resume.clone();
        for (key, stored) in page.entries {
            if let Some(prev) = &previous {
                if prev >= &key {
                    return Err(Divergence::Hard(format!(
                        "node={node} returned unordered scan keys {:?} then {:?}",
                        String::from_utf8_lossy(prev),
                        String::from_utf8_lossy(&key)
                    )));
                }
            }
            let now_ms = profile_physical_now_ms();
            while expected.peek().is_some_and(|(rkey, _)| ***rkey < *key) {
                let (rkey, rexp) = expected.next().expect("peeked entry");
                fold(
                    check_expected(Api::Replica, rkey, rexp, None, now_ms, model),
                    &mut counts,
                    &mut soft,
                    &mut soft_keys,
                )?;
            }
            match expected.peek() {
                Some((rkey, _)) if ***rkey == *key => {
                    let (rkey, rexp) = expected.next().expect("peeked entry");
                    fold(
                        check_expected(Api::Replica, rkey, rexp, Some(&stored), now_ms, model),
                        &mut counts,
                        &mut soft,
                        &mut soft_keys,
                    )?;
                }
                _ => {
                    return Err(Divergence::Hard(format!(
                        "node={node} scan returned unexpected extra key {:?} version {:?}",
                        String::from_utf8_lossy(&key),
                        stored.version
                    )))
                }
            }
            previous = Some(key);
        }
        cursor = match (resume, page.more, slice_hi) {
            (Some(resume), _, _) => Some(resume),
            (None, true, _) => Some(scan_successor(last.expect("truncated scan page must not be empty"))),
            (None, false, Some(hi)) => Some(hi),
            (None, false, None) => break,
        };
    }
    let now_ms = profile_physical_now_ms();
    for (rkey, rexp) in expected {
        fold(
            check_expected(Api::Replica, rkey, rexp, None, now_ms, model),
            &mut counts,
            &mut soft,
            &mut soft_keys,
        )?;
    }
    match soft {
        Some(reason) => Err(Divergence::Soft(format!("{soft_keys} unconverged key(s); first: {reason}"))),
        None => Ok(counts),
    }
}

async fn verify_all_nodes(
    transport: &vals_cluster::NetTransport,
    ids: &[u32],
    zone: vals_cluster::ZoneId,
    reference: &Reference,
    model: &Model,
    retry: Duration,
    converge: Duration,
    poll: Duration,
) -> Counts {
    let mut totals = Counts::default();
    for node in ids {
        let deadline = Instant::now() + converge;
        let began = Instant::now();
        loop {
            match verify_node_stream(transport, *node, zone, reference, model, retry).await {
                Ok(counts) => {
                    println!(
                        "PROFILECHECK VERIFY NODE node={node} live={} tombstones={} collected={} elapsed_s={:.3}",
                        counts.live, counts.tombstones, counts.collected, began.elapsed().as_secs_f64()
                    );
                    totals.live += counts.live;
                    totals.tombstones += counts.tombstones;
                    totals.collected += counts.collected;
                    break;
                }
                Err(Divergence::Hard(reason)) => panic!("PROFILECHECK CORRECTNESS final scan node={node}: {reason}"),
                Err(Divergence::Soft(reason)) => {
                    assert!(
                        Instant::now() < deadline,
                        "node={node} did not converge within {}s: {reason}",
                        converge.as_secs()
                    );
                    eprintln!(
                        "PROFILECHECK VERIFY RETRY node={node} waited_s={:.1} reason={reason}",
                        began.elapsed().as_secs_f64()
                    );
                    profile_sleep(poll).await;
                }
            }
        }
    }
    totals
}

#[derive(Default)]
struct PendingDivergence {
    keys: std::collections::BTreeMap<Vec<u8>, (Instant, String)>,
}

impl PendingDivergence {
    fn note(&mut self, key: &[u8], reason: String) {
        match self.keys.get_mut(key) {
            Some(entry) => entry.1 = reason,
            None => {
                self.keys.insert(key.to_vec(), (Instant::now(), reason));
            }
        }
    }

    fn healed(&mut self, key: &[u8]) {
        self.keys.remove(key);
    }

    fn keys(&self) -> Vec<Vec<u8>> {
        self.keys.keys().cloned().collect()
    }

    fn oldest(&self) -> Option<(Vec<u8>, Instant, String)> {
        self.keys
            .iter()
            .min_by_key(|(_, (since, _))| *since)
            .map(|(key, (since, reason))| (key.clone(), *since, reason.clone()))
    }

    fn len(&self) -> usize {
        self.keys.len()
    }
}

fn track_reconciliation_destinations(
    pending: &mut [PendingDivergence],
    key: &[u8],
    acked: &[usize],
    omitted: &[usize],
) {
    for index in acked {
        pending[*index].healed(key);
    }
    for index in omitted {
        pending[*index].note(key, "reconciliation write was not acknowledged".into());
    }
}

type PendingOmissions =
    std::collections::BTreeMap<(u32, Vec<u8>), (Instant, vals_cluster::Version, Option<Vec<u8>>)>;

fn clean_finalization_allowed(pending: &[PendingDivergence], omissions: &PendingOmissions) -> bool {
    omissions.is_empty() && pending.iter().all(|node| node.len() == 0)
}

fn clean_state_header(
    round: usize,
    source: &VersionSource,
    reference: &Reference,
    pending: &[PendingDivergence],
    omissions: &PendingOmissions,
) -> StateHeader {
    assert!(
        clean_finalization_allowed(pending, omissions),
        "profilecheck cannot save clean state with unresolved replica debt"
    );
    state_header(round, true, source, reference)
}

fn profile_client_config() -> vals_cluster::ClientConfig {
    let mut config = vals_cluster::ClientConfig::default();
    config.connect_deadline =
        Some(Duration::from_secs(env_u64("VALS_PROFILECHECK_CONNECT_DEADLINE_SECS", 5).max(1)));
    config.rpc_deadline =
        Some(Duration::from_secs(env_u64("VALS_PROFILECHECK_RPC_DEADLINE_SECS", 15).max(1)));
    config.validate().expect("bounded profilecheck client config")
}

fn classify_planned_ack(
    planned: vals_cluster::Version,
    ack: &vals_cluster::Applied,
) -> Result<Option<vals_cluster::Version>, String> {
    match ack {
        vals_cluster::Applied::Won => Ok(None),
        vals_cluster::Applied::Lost(existing) if *existing <= planned => Ok(None),
        vals_cluster::Applied::Lost(existing) => Ok(Some(*existing)),
        vals_cluster::Applied::Rejected => {
            Err(format!("replica rejected the planned candidate {planned:?}"))
        }
    }
}

#[derive(Default)]
struct PlannedOutcome {
    winner: Option<vals_cluster::Version>,
    acked: Vec<usize>,
    omitted: Vec<usize>,
    operations: u64,
    omitted_writes: u64,
    reconciled: u64,
}

fn reconciliation_version(
    source: &mut VersionSource,
    allow_reconciliation: bool,
    observed: vals_cluster::Version,
    planned: vals_cluster::Version,
    key: &[u8],
    round: usize,
    label: &str,
) -> vals_cluster::Version {
    assert!(
        allow_reconciliation,
        "PROFILECHECK CORRECTNESS {label} key={:?} round={round}: unknown winner {observed:?} beat planned {planned:?}; reconciliation is allowed only for state loaded dirty with VALS_PROFILECHECK_ALLOW_DIRTY_RESUME=1",
        String::from_utf8_lossy(key)
    );
    source.observe_at_least(observed.hlc);
    let replacement = source.next_triple(profile_physical_now_ms())[2];
    assert!(replacement > observed, "reconciling version {replacement:?} must beat the observed winner {observed:?}");
    replacement
}

fn record_reconciliations(total: &mut u64, added: u64, maximum: u64) {
    *total = total.checked_add(added).expect("resume reconciliation count overflow");
    assert!(*total <= maximum, "resume reconciliation exceeded {maximum} keys");
}

fn report_first_mutation(reported: &mut bool, label: &str, round: usize) {
    if !*reported {
        use std::io::Write as _;
        println!("PROFILECHECK FIRST_MUTATION label={label} round={round}");
        std::io::stdout().flush().expect("flush first mutation marker");
        *reported = true;
    }
}

async fn apply_winner_everywhere(
    transport: &vals_cluster::NetTransport,
    ids: &[u32],
    zone: vals_cluster::ZoneId,
    key: &[u8],
    value: Option<&[u8]>,
    version: vals_cluster::Version,
    retry: Duration,
    round: usize,
    label: &str,
) -> (PlannedOutcome, Option<vals_cluster::Version>) {
    let mut outcome = PlannedOutcome { winner: Some(version), ..PlannedOutcome::default() };
    let mut newer: Option<vals_cluster::Version> = None;
    for (index, node) in ids.iter().enumerate() {
        match profile_apply_retry(transport, *node, zone, key, value, version, retry).await {
            Ok(ack) => {
                match classify_planned_ack(version, &ack) {
                    Ok(None) => {}
                    Ok(Some(existing)) => {
                        newer = Some(newer.map_or(existing, |current: vals_cluster::Version| current.max(existing)));
                    }
                    Err(reason) => panic!("PROFILECHECK CORRECTNESS {label} node={node} round={round}: {reason}"),
                }
                outcome.operations += 1;
                outcome.acked.push(index);
            }
            Err(e) => {
                outcome.omitted_writes += 1;
                outcome.omitted.push(index);
                eprintln!("PROFILECHECK TRANSIENT omitted_{label} node={node} round={round} error={e:?}");
            }
        }
    }
    (outcome, newer)
}

#[allow(clippy::too_many_arguments)]
async fn apply_planned_winner(
    transport: &vals_cluster::NetTransport,
    ids: &[u32],
    zone: vals_cluster::ZoneId,
    key: &[u8],
    value: Option<&[u8]>,
    version: vals_cluster::Version,
    source: &mut VersionSource,
    allow_reconciliation: bool,
    retry: Duration,
    round: usize,
    label: &str,
) -> PlannedOutcome {
    let (mut outcome, newer) =
        apply_winner_everywhere(transport, ids, zone, key, value, version, retry, round, label).await;
    let Some(existing) = newer else { return outcome };
    let replacement = reconciliation_version(
        source, allow_reconciliation, existing, version, key, round, label,
    );
    eprintln!(
        "PROFILECHECK RECONCILE {label} key={:?} round={round} observed_winner={existing:?} above planned={version:?}",
        String::from_utf8_lossy(key)
    );
    let (retry_outcome, still_newer) =
        apply_winner_everywhere(transport, ids, zone, key, value, replacement, retry, round, label).await;
    assert!(
        still_newer.is_none(),
        "PROFILECHECK CORRECTNESS {label} key={:?} round={round}: an unknown winner {:?} beat the reconciling candidate {replacement:?}",
        String::from_utf8_lossy(key),
        still_newer
    );
    outcome.operations += retry_outcome.operations;
    outcome.omitted_writes += retry_outcome.omitted_writes;
    outcome.acked = retry_outcome.acked;
    outcome.omitted = retry_outcome.omitted;
    outcome.winner = Some(replacement);
    outcome.reconciled = 1;
    outcome
}

#[allow(clippy::too_many_arguments)]
async fn reconcile_unknown_winner(
    transport: &vals_cluster::NetTransport,
    ids: &[u32],
    zone: vals_cluster::ZoneId,
    key: &[u8],
    observed: vals_cluster::Version,
    source: &mut VersionSource,
    allow_reconciliation: bool,
    retry: Duration,
    round: usize,
    quorum: usize,
) -> (vals_cluster::Version, Vec<u8>, PlannedOutcome) {
    let value = format!("reconciled-{round}").into_bytes();
    let version = reconciliation_version(
        source,
        allow_reconciliation,
        observed,
        observed,
        key,
        round,
        "resume_reconcile",
    );
    let outcome = apply_planned_winner(
        transport,
        ids,
        zone,
        key,
        Some(value.as_slice()),
        version,
        source,
        allow_reconciliation,
        retry,
        round,
        "resume_reconcile",
    ).await;
    assert!(
        outcome.acked.len() >= quorum,
        "resume reconciliation of key={:?} reached only {}/{quorum} replicas",
        String::from_utf8_lossy(key),
        outcome.acked.len()
    );
    let winner = outcome.winner.expect("reconciled winner");
    println!(
        "PROFILECHECK RESUME_RECONCILE key={:?} observed={observed:?} replaced_with={winner:?}",
        String::from_utf8_lossy(key)
    );
    (winner, value, outcome)
}

pub(super) fn workload_profilecheck(peers_arg: &str, rounds: usize, expected_secs: Option<u64>) {
    use std::io::Read as _;
    use std::net::SocketAddr;
    use vals_cluster::{Applied, BatchAck, BatchEntry, Coordinator, NetTransport, Ring, Transport, DEFAULT_ZONE};

    let nodes: Vec<(u32, Vec<SocketAddr>)> = peers_arg
        .split(';')
        .filter(|s| !s.trim().is_empty())
        .map(|s| {
            let (id, addrs) = s.split_once('@').expect("peer must be id@addr[,addr...]");
            (
                id.trim().parse().expect("peer id"),
                addrs.split(',').map(|a| a.trim().parse().expect("peer addr")).collect(),
            )
        })
        .collect();
    assert!(!nodes.is_empty(), "profilecheck needs at least one node");
    let ids: Vec<u32> = nodes.iter().map(|(id, _)| *id).collect();
    let quorum = ids.len() / 2 + 1;
    let client_config = profile_client_config();
    let transport = NetTransport::connect_sharded_with_config(nodes.clone(), client_config)
        .expect("bounded profilecheck client config");
    let mut ring = Ring::new(ids.len());
    ring.set_members_weighted(ids.iter().copied().map(|id| (id, 1)));
    let coordinator = Coordinator::new(
        0x0194,
        ring,
        quorum,
        NetTransport::connect_sharded_with_config(nodes, client_config)
            .expect("bounded profilecheck client config"),
    );

    let zone_id = std::env::var("VALS_PROFILECHECK_ZONE").ok().and_then(|s| s.parse().ok()).unwrap_or(DEFAULT_ZONE);
    let state_path = std::env::var_os("VALS_PROFILECHECK_STATE").map(std::path::PathBuf::from);
    let verify_only = std::env::var_os("VALS_PROFILECHECK_VERIFY_ONLY").is_some();
    let model = Model {
        seed_keys: env_u64("VALS_PROFILECHECK_SEED_KEYS", 0) as usize,
        workload_keys: (env_u64("VALS_PROFILECHECK_WORKLOAD_KEYS", 25_000) as usize).max(1),
        gc_grace_ms: env_u64("VALS_PROFILECHECK_GC_GRACE_SECS", 900).saturating_mul(1000),
        skew_margin_ms: env_u64("VALS_PROFILECHECK_CLOCK_SKEW_MS", 1000),
    };
    let direct_retry = Duration::from_secs(env_u64("VALS_PROFILECHECK_DIRECT_RETRY_SECS", 5));
    let coordinator_retry = Duration::from_secs(env_u64("VALS_PROFILECHECK_COORD_RETRY_SECS", 300));
    let verify_retry = Duration::from_secs(env_u64("VALS_PROFILECHECK_VERIFY_RETRY_SECS", 300));
    let verify_converge = Duration::from_secs(env_u64("VALS_PROFILECHECK_VERIFY_CONVERGE_SECS", 900));
    let verify_poll = Duration::from_secs(env_u64("VALS_PROFILECHECK_VERIFY_POLL_SECS", 10).max(1));
    let node_converge = Duration::from_secs(env_u64("VALS_PROFILECHECK_NODE_CONVERGE_SECS", 900));
    let max_node_outage = Duration::from_secs(env_u64("VALS_PROFILECHECK_MAX_NODE_OUTAGE_SECS", 900));
    let sample_keys = env_u64("VALS_PROFILECHECK_SAMPLE_KEYS", 64) as usize;
    let node_sample_keys = env_u64("VALS_PROFILECHECK_NODE_SAMPLE_KEYS", 16) as usize;
    let sample_every = (env_u64("VALS_PROFILECHECK_SAMPLE_EVERY", 32) as usize).max(1);
    let checkpoint_period = Duration::from_secs(env_u64("VALS_PROFILECHECK_CHECKPOINT_SECS", 900).max(1));
    let seed_batch = (env_u64("VALS_PROFILECHECK_SEED_BATCH", 256) as usize).max(1).min(4096);
    let max_model_rss_kib = env_u64("VALS_PROFILECHECK_MAX_MODEL_RSS_MIB", 4096).saturating_mul(1024);
    let max_pending_keys = env_u64("VALS_PROFILECHECK_MAX_PENDING_KEYS", 256) as usize;
    let gc_probe_period = Some(env_u64("VALS_PROFILECHECK_GC_PROBE_SECS", 0)).filter(|s| *s > 0).map(Duration::from_secs);
    let gc_probe_keys = (env_u64("VALS_PROFILECHECK_GC_PROBE_KEYS", 64) as usize).max(1);
    let gc_overdue_ms = env_u64(
        "VALS_PROFILECHECK_GC_OVERDUE_SECS",
        2 * (model.gc_grace_ms / 1000).min(3600) + 300,
    )
    .saturating_mul(1000);
    let gc_max_held_pct = env_u64("VALS_PROFILECHECK_GC_MAX_HELD_PCT", 100).min(100);
    let probe_node: Option<u32> = std::env::var("VALS_PROFILECHECK_PROBE_NODE").ok().and_then(|s| s.parse().ok());
    let probe_deadline = Duration::from_secs(env_u64("VALS_PROFILECHECK_PROBE_SECS", 20).max(1));
    let dirty_resume_value = std::env::var_os("VALS_PROFILECHECK_ALLOW_DIRTY_RESUME");
    let allow_dirty_resume = dirty_resume_requested(dirty_resume_value.as_deref());
    let mut reference = Reference::new();
    let mut loaded = StateHeader::default();
    let mut state_was_loaded = false;
    if let Some(path) = &state_path {
        if path.exists() {
            state_was_loaded = true;
            let mut bytes = Vec::new();
            std::fs::File::open(path).expect("open profilecheck state").read_to_end(&mut bytes).expect("read profilecheck state");
            (loaded, reference) = decode_state(&bytes, zone_id, &model).unwrap_or_else(|e| panic!("invalid profilecheck state {}: {e}", path.display()));
            assert!(
                loaded.clean || allow_dirty_resume,
                "profilecheck state {} was not cleanly finalized (round {}): uncheckpointed winners may exist; set VALS_PROFILECHECK_ALLOW_DIRTY_RESUME=1 to reconcile them at write time",
                path.display(),
                loaded.round
            );
            println!(
                "PROFILECHECK RESUME round={} clean={} high_hlc={} keys={}",
                loaded.round, u8::from(loaded.clean), loaded.high_hlc, reference.len()
            );
        }
    }
    let initial_round = loaded.round;
    let allow_reconciliation = reconciliation_is_allowed(
        state_was_loaded,
        loaded.clean,
        dirty_resume_value.as_deref(),
    );
    let max_reconciles = env_u64("VALS_PROFILECHECK_MAX_RECONCILES", 65_536);
    if verify_only {
        assert!(state_path.is_some(), "verify-only requires VALS_PROFILECHECK_STATE");
        assert!(!reference.is_empty(), "verify-only refuses an empty reference");
    }

    let started = Instant::now();
    let deadline = expected_secs.map(|s| started + Duration::from_secs(s));
    let mut operations = 0u64;
    let mut checkpoints = 0u64;
    let mut omitted_replica_writes = 0u64;
    let mut winner_tombstones = 0u64;
    let mut resurrections = 0u64;
    let mut cold_overwrites = 0u64;
    let mut retired_keys = 0u64;
    let mut reconciled_winners = 0u64;
    let mut omitted_live_cold_seed = 0u64;
    let mut healed_live_cold_seed = 0u64;
    let mut deferred_cold_overwrites = 0u64;
    let mut gc_probes = 0u64;
    let mut gc_totals = GcProbe::default();
    let mut versions_source = VersionSource::resumed_from(loaded.high_hlc, &reference);
    let mut node_last_ok: Vec<Instant> = ids.iter().map(|_| Instant::now()).collect();
    let mut pending: Vec<PendingDivergence> = ids.iter().map(|_| PendingDivergence::default()).collect();
    let mut pending_omissions: PendingOmissions = Default::default();
    let mut verify_counts = Counts::default();

    vals_rt::block_on(
        async {
            if let Some(node) = probe_node {
                eprintln!("PROFILECHECK PROBE START node={node} zone={zone_id} bound_s={}", probe_deadline.as_secs());
                let deadline = Instant::now() + probe_deadline;
                let key = b"__profilecheck_readiness_probe__";
                loop {
                    assert!(Instant::now() < deadline, "readiness probe deadline for node={node} zone={zone_id}");
                    match vals_cluster::Transport::read(&transport, node, zone_id, key).await {
                        Ok(_) => {
                            let hi = direct_slice_hi(zone_id, None);
                            match vals_cluster::Transport::scan(&transport, node, zone_id, None, hi.as_deref(), 1).await {
                                Ok(page) => {
                                    println!(
                                        "PROFILECHECK PROBE OK node={node} zone={zone_id} scan_entries={} elapsed_s={:.3}",
                                        page.entries.len(), started.elapsed().as_secs_f64()
                                    );
                                    return;
                                }
                                Err(vals_cluster::TransportError::StaleEpoch(epoch)) => {
                                    refresh_direct_routes(&transport, node, epoch).await;
                                }
                                Err(e) => eprintln!("PROFILECHECK PROBE RETRY node={node} scan error={e:?}"),
                            }
                        }
                        Err(vals_cluster::TransportError::StaleEpoch(epoch)) => {
                            refresh_direct_routes(&transport, node, epoch).await;
                        }
                        Err(e) => eprintln!("PROFILECHECK PROBE RETRY node={node} read error={e:?}"),
                    }
                    profile_retry_delay().await;
                }
            }

            let refresh_deadline = Instant::now() + coordinator_retry;
            loop {
                match coordinator.refresh_ring(ids[0]).await {
                    Ok(_) => break,
                    Err(e) if Instant::now() < refresh_deadline => { let _ = e; profile_retry_delay().await; }
                    Err(e) => panic!("profilecheck could not refresh ring before workload: {e:?}"),
                }
            }
            refresh_direct_routes(&transport, ids[0], coordinator.ring_epoch()).await;

            if verify_only {
                verify_counts = verify_all_nodes(
                    &transport,
                    &ids,
                    zone_id,
                    &reference,
                    &model,
                    verify_retry,
                    verify_converge,
                    verify_poll,
                )
                .await;
                return;
            }

            if let Some(path) = &state_path {
                save_state(
                    path,
                    state_header(initial_round, false, &versions_source, &reference),
                    zone_id,
                    &model,
                    &reference,
                );
            }
            let mut first_mutation_reported = false;

            if initial_round == 0 && reference.is_empty() {
                let seed_version = versions_source.next_triple(profile_physical_now_ms())[2];
                for base in (0..model.seed_keys).step_by(seed_batch) {
                    let end = base.saturating_add(seed_batch).min(model.seed_keys);
                    let rows: Vec<(Vec<u8>, Vec<u8>)> = (base..end)
                        .map(|i| (seed_key(i), format!("seed-value-{i}").into_bytes()))
                        .collect();
                    let entries: Vec<BatchEntry<'_>> = rows.iter().map(|(key, value)| BatchEntry {
                        key,
                        value: Some(value),
                        version: seed_version,
                    }).collect();
                    let mut seed_newer = vec![None; rows.len()];
                    for node in &ids {
                        let deadline = Instant::now() + coordinator_retry;
                        loop {
                            let acks = transport.apply_batch(*node, zone_id, true, &entries).await;
                            if acks.iter().all(|ack| matches!(ack, BatchAck::Applied(Applied::Won | Applied::Lost(_)))) {
                                for (position, ack) in acks.iter().enumerate() {
                                    let BatchAck::Applied(applied) = ack else { unreachable!() };
                                    match classify_planned_ack(seed_version, applied) {
                                        Ok(None) => {}
                                        Ok(Some(existing)) => {
                                            seed_newer[position] = Some(seed_newer[position].map_or(
                                                existing,
                                                |current: vals_cluster::Version| current.max(existing),
                                            ));
                                        }
                                        Err(reason) => panic!(
                                            "PROFILECHECK CORRECTNESS seed apply node={node} key={:?}: {reason}",
                                            String::from_utf8_lossy(&rows[position].0)
                                        ),
                                    }
                                    if matches!(applied, Applied::Won) {
                                        report_first_mutation(&mut first_mutation_reported, "seed", 0);
                                    }
                                }
                                operations += acks.len() as u64;
                                break;
                            }
                            if let Some(epoch) = acks.iter().filter_map(|ack| match ack { BatchAck::Stale(epoch) => Some(*epoch), _ => None }).max() {
                                refresh_direct_routes(&transport, *node, epoch).await;
                            }
                            assert!(Instant::now() < deadline, "seed BatchApply retry deadline node={node}: {acks:?}");
                            profile_retry_delay().await;
                        }
                    }
                    drop(entries);
                    for ((key, value), newer) in rows.into_iter().zip(seed_newer) {
                        let winner = if let Some(existing) = newer {
                            let replacement = reconciliation_version(
                                &mut versions_source,
                                allow_reconciliation,
                                existing,
                                seed_version,
                                &key,
                                0,
                                "seed_reconcile",
                            );
                            let outcome = apply_planned_winner(
                                &transport,
                                &ids,
                                zone_id,
                                &key,
                                Some(&value),
                                replacement,
                                &mut versions_source,
                                allow_reconciliation,
                                coordinator_retry,
                                0,
                                "seed_reconcile",
                            ).await;
                            operations += outcome.operations;
                            omitted_replica_writes += outcome.omitted_writes;
                            assert!(outcome.acked.len() >= quorum, "seed reconciliation reached only {}/{quorum} replicas", outcome.acked.len());
                            track_reconciliation_destinations(&mut pending, &key, &outcome.acked, &outcome.omitted);
                            record_reconciliations(&mut reconciled_winners, 1, max_reconciles);
                            outcome.winner.expect("reconciled seed winner")
                        } else {
                            seed_version
                        };
                        reference.insert(key, (winner, Some(value)));
                    }
                }
                if let Some(path) = &state_path {
                    save_state(path, state_header(0, false, &versions_source, &reference), zone_id, &model, &reference);
                }
                println!(
                    "PROFILECHECK SEEDED keys={} rss_kib={} elapsed_s={:.3}",
                    reference.len(), process_rss_kib(), started.elapsed().as_secs_f64()
                );
            }

            let mut round = initial_round;
            let target_round = initial_round.saturating_add(rounds);
            let mut next_checkpoint = Instant::now() + checkpoint_period;
            let mut next_gc_probe = gc_probe_period.map(|period| Instant::now() + period);
            let mut gc_probe_offset = 0usize;
            while round < target_round || deadline.is_some_and(|d| Instant::now() < d) {
                let versions = versions_source.next_triple(profile_physical_now_ms());
                let plan = round_plan(round, &model);

                let mut replica_acks = 0usize;
                let mut observed_newer: Option<vals_cluster::Version> = None;
                for (index, node) in ids.iter().enumerate() {
                    let mut node_acked = false;
                    for i in [2usize, 0, 1, 2] {
                        match profile_apply_retry(&transport, *node, zone_id, &plan.direct_key, plan.direct_values[i].as_deref(), versions[i], direct_retry).await {
                            Ok(ack) => {
                                if i == 2 {
                                    match classify_planned_ack(versions[2], &ack) {
                                        Ok(None) => {}
                                        Ok(Some(existing)) => {
                                            observed_newer = Some(observed_newer.map_or(existing, |current: vals_cluster::Version| current.max(existing)));
                                        }
                                        Err(reason) => panic!("PROFILECHECK CORRECTNESS direct apply node={node} round={round}: {reason}"),
                                    }
                                } else {
                                    assert!(matches!(ack, Applied::Won | Applied::Lost(_)), "unconditional Apply got {ack:?}");
                                }
                                if matches!(ack, Applied::Won) {
                                    report_first_mutation(&mut first_mutation_reported, "direct", round);
                                }
                                operations += 1;
                                node_acked = true;
                            }
                            Err(e) => {
                                omitted_replica_writes += 1;
                                eprintln!("PROFILECHECK TRANSIENT omitted_apply node={node} round={round} error={e:?}");
                                break;
                            }
                        }
                    }
                    if node_acked {
                        node_last_ok[index] = Instant::now();
                    }
                    replica_acks += usize::from(node_acked);
                }
                assert!(replica_acks >= quorum, "direct Apply reached only {replica_acks}/{quorum} required replicas");
                let mut direct_winner = versions[2];
                if let Some(existing) = observed_newer {
                    let replacement = reconciliation_version(
                        &mut versions_source,
                        allow_reconciliation,
                        existing,
                        versions[2],
                        &plan.direct_key,
                        round,
                        "direct_reconcile",
                    );
                    let outcome = apply_planned_winner(
                        &transport,
                        &ids,
                        zone_id,
                        &plan.direct_key,
                        plan.direct_values[2].as_deref(),
                        replacement,
                        &mut versions_source,
                        allow_reconciliation,
                        direct_retry,
                        round,
                        "direct_reconcile",
                    ).await;
                    operations += outcome.operations;
                    omitted_replica_writes += outcome.omitted_writes;
                    assert!(outcome.acked.len() >= quorum, "direct reconciliation reached only {}/{quorum} replicas", outcome.acked.len());
                    track_reconciliation_destinations(&mut pending, &plan.direct_key, &outcome.acked, &outcome.omitted);
                    for index in &outcome.acked { node_last_ok[*index] = Instant::now(); }
                    direct_winner = outcome.winner.expect("reconciled winner");
                    record_reconciliations(&mut reconciled_winners, 1, max_reconciles);
                }
                if plan.direct_values[2].is_none() {
                    winner_tombstones += 1;
                }
                if reference.get(&plan.direct_key).is_some_and(|(_, value)| value.is_none())
                    && plan.direct_values[2].is_some()
                {
                    resurrections += 1;
                }
                reference.insert(plan.direct_key.clone(), (direct_winner, plan.direct_values[2].clone()));

                let entries: Vec<BatchEntry<'_>> = [2usize, 0, 1, 2].iter().map(|&i| BatchEntry {
                    key: &plan.batch_key,
                    value: plan.batch_values[i].as_deref(),
                    version: versions[i],
                }).collect();
                let mut batch_replicas = 0usize;
                let mut batch_newer: Option<vals_cluster::Version> = None;
                for (index, node) in ids.iter().enumerate() {
                    let retry_deadline = Instant::now() + direct_retry;
                    loop {
                        let acks = transport.apply_batch(*node, zone_id, round % 2 == 0, &entries).await;
                        if acks.iter().all(|ack| matches!(ack, BatchAck::Applied(Applied::Won | Applied::Lost(_)))) {
                            for (position, ack) in acks.iter().enumerate() {
                                if position != 0 && position != 3 { continue; }
                                if let BatchAck::Applied(applied) = ack {
                                    match classify_planned_ack(versions[2], applied) {
                                        Ok(None) => {}
                                        Ok(Some(existing)) => {
                                            batch_newer = Some(batch_newer.map_or(existing, |current: vals_cluster::Version| current.max(existing)));
                                        }
                                        Err(reason) => panic!("PROFILECHECK CORRECTNESS batch apply node={node} round={round}: {reason}"),
                                    }
                                }
                            }
                            operations += acks.len() as u64;
                            batch_replicas += 1;
                            node_last_ok[index] = Instant::now();
                            break;
                        }
                        if let Some(epoch) = acks.iter().filter_map(|ack| match ack { BatchAck::Stale(epoch) => Some(*epoch), _ => None }).max() {
                            refresh_direct_routes(&transport, *node, epoch).await;
                        }
                        if Instant::now() >= retry_deadline {
                            omitted_replica_writes += acks.len() as u64;
                            eprintln!("PROFILECHECK TRANSIENT omitted_batch node={node} round={round} acks={acks:?}");
                            break;
                        }
                        profile_retry_delay().await;
                    }
                }
                assert!(batch_replicas >= quorum, "BatchApply reached only {batch_replicas}/{quorum} required replicas");
                let mut batch_winner = versions[2];
                if let Some(existing) = batch_newer {
                    let replacement = reconciliation_version(
                        &mut versions_source,
                        allow_reconciliation,
                        existing,
                        versions[2],
                        &plan.batch_key,
                        round,
                        "batch_reconcile",
                    );
                    let outcome = apply_planned_winner(
                        &transport,
                        &ids,
                        zone_id,
                        &plan.batch_key,
                        plan.batch_values[2].as_deref(),
                        replacement,
                        &mut versions_source,
                        allow_reconciliation,
                        direct_retry,
                        round,
                        "batch_reconcile",
                    ).await;
                    operations += outcome.operations;
                    omitted_replica_writes += outcome.omitted_writes;
                    assert!(outcome.acked.len() >= quorum, "batch reconciliation reached only {}/{quorum} replicas", outcome.acked.len());
                    track_reconciliation_destinations(&mut pending, &plan.batch_key, &outcome.acked, &outcome.omitted);
                    for index in &outcome.acked { node_last_ok[*index] = Instant::now(); }
                    batch_winner = outcome.winner.expect("reconciled winner");
                    record_reconciliations(&mut reconciled_winners, 1, max_reconciles);
                }
                if plan.batch_values[2].is_none() {
                    winner_tombstones += 1;
                }
                if reference.get(&plan.batch_key).is_some_and(|(_, value)| value.is_none())
                    && plan.batch_values[2].is_some()
                {
                    resurrections += 1;
                }
                reference.insert(plan.batch_key.clone(), (batch_winner, plan.batch_values[2].clone()));

                let cv = coordinator_write_retry(&coordinator, zone_id, &plan.coordinator_key, &plan.coordinator_value, coordinator_retry).await;
                versions_source.observe_at_least(cv.hlc);
                let got = coordinator_read_retry(&coordinator, zone_id, &plan.coordinator_key, coordinator_retry).await.expect("coordinator write disappeared");
                assert_eq!(got.version, cv);
                assert_eq!(got.value.as_deref(), Some(plan.coordinator_value.as_slice()));
                reference.insert(plan.coordinator_key.clone(), (cv, Some(plan.coordinator_value.clone())));
                operations += 2;

                if let Some((key, value)) = &plan.seed_overwrite {
                    if pending_omissions.keys().any(|(_, pending_key)| pending_key == key) {
                        deferred_cold_overwrites += 1;
                    } else {
                        let outcome = apply_planned_winner(
                            &transport,
                            &ids,
                            zone_id,
                            key,
                            Some(value),
                            versions[2],
                            &mut versions_source,
                            allow_reconciliation,
                            direct_retry,
                            round,
                            "cold_overwrite",
                        ).await;
                        operations += outcome.operations;
                        omitted_replica_writes += outcome.omitted_writes;
                        record_reconciliations(&mut reconciled_winners, outcome.reconciled, max_reconciles);
                        assert!(outcome.acked.len() >= quorum, "cold seed overwrite reached only {}/{quorum} required replicas", outcome.acked.len());
                        for index in &outcome.acked { node_last_ok[*index] = Instant::now(); }
                        let winner = outcome.winner.expect("cold overwrite winner");
                        cold_overwrites += 1;
                        for index in &outcome.omitted {
                            omitted_live_cold_seed += 1;
                            pending_omissions.insert(
                                (ids[*index], key.clone()),
                                (Instant::now(), winner, Some(value.clone())),
                            );
                        }
                        reference.insert(key.clone(), (winner, Some(value.clone())));
                    }
                }

                if let Some(key) = &plan.retired_key {
                    let live = format!("retired-live-{round}").into_bytes();
                    let mut acks = 0usize;
                    for (index, node) in ids.iter().enumerate() {
                        let mut node_acked = false;
                        for (value, version) in [(Some(live.as_slice()), versions[0]), (None, versions[2])] {
                            match profile_apply_retry(&transport, *node, zone_id, key, value, version, direct_retry).await {
                                Ok(ack) => {
                                    if version == versions[2] {
                                        match classify_planned_ack(version, &ack) {
                                            Ok(None) => {}
                                            Ok(Some(existing)) => panic!(
                                                "PROFILECHECK CORRECTNESS retire node={node} round={round}: unknown winner {existing:?} beat the planned tombstone {version:?}"
                                            ),
                                            Err(reason) => panic!("PROFILECHECK CORRECTNESS retire node={node} round={round}: {reason}"),
                                        }
                                    }
                                    operations += 1;
                                    node_acked = true;
                                }
                                Err(e) => {
                                    omitted_replica_writes += 1;
                                    eprintln!("PROFILECHECK TRANSIENT omitted_retire node={node} round={round} error={e:?}");
                                    node_acked = false;
                                    break;
                                }
                            }
                        }
                        if node_acked {
                            acks += 1;
                            node_last_ok[index] = Instant::now();
                        }
                    }
                    assert!(acks >= quorum, "retiring delete reached only {acks}/{quorum} required replicas");
                    winner_tombstones += 1;
                    retired_keys += 1;
                    reference.insert(key.clone(), (versions[2], None));
                }

                round += 1;
                for (index, node) in ids.iter().enumerate() {
                    let outage = node_last_ok[index].elapsed();
                    assert!(
                        outage <= max_node_outage,
                        "node={node} accepted no direct write for {}s, above the {}s single-node outage bound",
                        outage.as_secs(),
                        max_node_outage.as_secs()
                    );
                }

                let sample_due = round % sample_every == 0 || round == target_round;
                if sample_due {
                    let now_ms = profile_physical_now_ms();
                    let offset = round % reference.len().max(1);
                    let sample: Vec<(Vec<u8>, (vals_cluster::Version, Option<Vec<u8>>))> = reference
                        .iter()
                        .cycle()
                        .skip(offset)
                        .take(sample_keys.min(reference.len()))
                        .map(|(key, expected)| (key.clone(), expected.clone()))
                        .collect();
                    let mut reconciled_this_sample: std::collections::BTreeMap<Vec<u8>, Vec<usize>> = Default::default();
                    for (key, expected) in &sample {
                        let got = coordinator_read_retry(&coordinator, zone_id, key, coordinator_retry).await;
                        if allow_reconciliation && got.as_ref().is_some_and(|got| got.version > expected.0) {
                            let observed = got.as_ref().expect("checked").version;
                            let (version, value, outcome) = reconcile_unknown_winner(
                                &transport, &ids, zone_id, key, observed, &mut versions_source, allow_reconciliation,
                                direct_retry, round, quorum,
                            ).await;
                            operations += outcome.operations;
                            omitted_replica_writes += outcome.omitted_writes;
                            record_reconciliations(&mut reconciled_winners, 1, max_reconciles);
                            reference.insert(key.clone(), (version, Some(value)));
                            track_reconciliation_destinations(
                                &mut pending,
                                key,
                                &outcome.acked,
                                &outcome.omitted,
                            );
                            reconciled_this_sample.insert(key.clone(), outcome.acked);
                            continue;
                        }
                        let _ = assert_expected(Api::PointRead, key, expected, got.as_ref(), now_ms, &model, "in rotating strong-read sample")
                            .unwrap_or_else(|reason| panic!("PROFILECHECK CORRECTNESS strong read: {reason}"));
                    }
                    for (index, node) in ids.iter().enumerate() {
                        let mut recheck: Vec<(Vec<u8>, (vals_cluster::Version, Option<Vec<u8>>))> = Vec::new();
                        for key in pending[index].keys() {
                            match reference.get(&key) {
                                Some(expected) => recheck.push((key, expected.clone())),
                                None => pending[index].healed(&key),
                            }
                        }
                        let checks = recheck
                            .iter()
                            .chain(sample.iter().take(node_sample_keys))
                            .map(|(key, expected)| (key.clone(), expected.clone()))
                            .collect::<Vec<_>>();
                        for (key, expected) in &checks {
                            if reconciled_this_sample.get(key).is_some_and(|acked| acked.contains(&index)) {
                                pending[index].healed(key);
                                continue;
                            }
                            match profile_node_read_retry(&transport, *node, zone_id, key, direct_retry).await {
                                Ok(got) => {
                                    match assert_expected(Api::Replica, key, expected, got.as_ref(), now_ms, &model, &format!("in per-node checkpoint sample node={node}")) {
                                        Ok(_) => pending[index].healed(key),
                                        Err(reason) => pending[index].note(key, reason),
                                    }
                                }
                                Err(e) => pending[index].note(key, format!("read error {e:?}")),
                            }
                        }
                        assert!(
                            pending[index].len() <= max_pending_keys,
                            "node={node} accumulated {} unhealed divergent keys, above the {max_pending_keys} tracking bound",
                            pending[index].len()
                        );
                        if let Some((key, since, reason)) = pending[index].oldest() {
                            eprintln!(
                                "PROFILECHECK NODE_DIVERGENT node={node} pending={} oldest_key={:?} for_s={:.1} reason={reason}",
                                pending[index].len(),
                                String::from_utf8_lossy(&key),
                                since.elapsed().as_secs_f64()
                            );
                            assert!(
                                since.elapsed() <= node_converge,
                                "node={node} key={:?} stayed divergent for {}s, above the {}s per-node convergence window: {reason}",
                                String::from_utf8_lossy(&key),
                                since.elapsed().as_secs(),
                                node_converge.as_secs()
                            );
                        }
                    }
                    let omissions: Vec<((u32, Vec<u8>), (Instant, vals_cluster::Version, Option<Vec<u8>>))> =
                        pending_omissions.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                    for ((node, key), (since, version, value)) in omissions {
                        let expected = (version, value.clone());
                        match profile_node_read_retry(&transport, node, zone_id, &key, direct_retry).await {
                            Ok(got) => {
                                match check_expected(Api::Replica, &key, &expected, got.as_ref(), now_ms, &model) {
                                    Ok(_) => {
                                        healed_live_cold_seed += 1;
                                        pending_omissions.remove(&(node, key.clone()));
                                        println!(
                                            "PROFILECHECK OMISSION_HEALED node={node} key={:?} version={version:?} waited_s={:.1}",
                                            String::from_utf8_lossy(&key),
                                            since.elapsed().as_secs_f64()
                                        );
                                    }
                                    Err(Divergence::Hard(reason)) => panic!(
                                        "PROFILECHECK CORRECTNESS omitted cold-seed winner on node={node}: {reason}"
                                    ),
                                    Err(Divergence::Soft(reason)) => {
                                        assert!(
                                            since.elapsed() <= node_converge,
                                            "node={node} never healed omitted cold-seed winner key={:?} within {}s: {reason}",
                                            String::from_utf8_lossy(&key),
                                            node_converge.as_secs()
                                        );
                                    }
                                }
                            }
                            Err(e) => assert!(
                                since.elapsed() <= node_converge,
                                "node={node} unreadable while healing omitted cold-seed winner key={:?} for {}s: {e:?}",
                                String::from_utf8_lossy(&key),
                                node_converge.as_secs()
                            ),
                        }
                    }
                    let before = coordinator_write_retry(&coordinator, zone_id, &plan.cas_key, b"before-cas", coordinator_retry).await;
                    let after = coordinator_cas_retry(&coordinator, zone_id, &plan.cas_key, b"after-cas", before, coordinator_retry).await;
                    assert!(after > before);
                    coordinator_cas_mismatch_retry(
                        &coordinator,
                        zone_id,
                        &plan.cas_key,
                        before,
                        after,
                        b"after-cas",
                        coordinator_retry,
                    ).await;
                    versions_source.observe_at_least(after.hlc);
                    reference.insert(plan.cas_key.clone(), (after, Some(b"after-cas".to_vec())));
                    operations += 3;
                }
                if next_gc_probe.is_some_and(|due| Instant::now() >= due) {
                    next_gc_probe = gc_probe_period.map(|period| Instant::now() + period);
                    let now_ms = profile_physical_now_ms();
                    let overdue: Vec<_> = reference
                        .iter()
                        .filter(|(_, (version, value))| {
                            value.is_none() && model.tombstone_is_overdue(*version, now_ms, gc_overdue_ms)
                        })
                        .collect();
                    let sample: Vec<_> = overdue
                        .iter()
                        .cycle()
                        .skip(gc_probe_offset % overdue.len().max(1))
                        .take(gc_probe_keys.min(overdue.len()))
                        .collect();
                    gc_probe_offset = gc_probe_offset.wrapping_add(gc_probe_keys);
                    if !sample.is_empty() {
                        gc_probes += 1;
                    }
                    for (index, node) in ids.iter().enumerate() {
                        let mut probe = GcProbe::default();
                        for (position, (key, expected)) in sample.iter().enumerate() {
                            match profile_node_read_retry(&transport, *node, zone_id, key, direct_retry).await {
                                Ok(got) => match check_expected(Api::Replica, key, expected, got.as_ref(), now_ms, &model) {
                                    Ok(counts) => {
                                        probe.collected += counts.collected;
                                        probe.held += counts.tombstones;
                                    }
                                    Err(Divergence::Hard(reason)) => panic!(
                                        "PROFILECHECK CORRECTNESS overdue-tombstone probe node={node}: {reason}"
                                    ),
                                    Err(Divergence::Soft(reason)) => {
                                        probe.stale += 1;
                                        pending[index].note(key, reason);
                                    }
                                },
                                Err(_) => {
                                    probe.unreadable += (sample.len() - position) as u64;
                                    break;
                                }
                            }
                        }
                        gc_totals.collected += probe.collected;
                        gc_totals.held += probe.held;
                        gc_totals.stale += probe.stale;
                        gc_totals.unreadable += probe.unreadable;
                        println!(
                            "PROFILECHECK GC_PROBE node={node} round={round} overdue={} sampled={} collected={} held={} stale={} unreadable={} overdue_after_s={}",
                            overdue.len(), sample.len(), probe.collected, probe.held, probe.stale, probe.unreadable,
                            (model.gc_grace_ms + model.skew_margin_ms + gc_overdue_ms) / 1000
                        );
                        if let Err(reason) = probe.verdict(gc_max_held_pct) {
                            panic!("PROFILECHECK GC node={node} round={round}: {reason}");
                        }
                    }
                }
                if Instant::now() >= next_checkpoint || round == target_round {
                    checkpoints += 1;
                    assert!(
                        reference.len() <= model.max_keys(),
                        "reference grew to {} keys above the bounded model maximum {}",
                        reference.len(),
                        model.max_keys()
                    );
                    let rss_kib = process_rss_kib();
                    assert!(
                        rss_kib <= max_model_rss_kib,
                        "driver RSS {rss_kib} KiB exceeded the configured model budget {max_model_rss_kib} KiB"
                    );
                    if let Some(path) = &state_path {
                        save_state(path, state_header(round, false, &versions_source, &reference), zone_id, &model, &reference);
                    }
                    next_checkpoint = Instant::now() + checkpoint_period;
                    println!("PROFILECHECK CHECKPOINT round={round} keys={} operations={operations} omitted_replica_writes={omitted_replica_writes} winner_tombstones={winner_tombstones} pending_omissions={} reconciled_winners={reconciled_winners} rss_kib={rss_kib}", reference.len(), pending_omissions.len());
                    {
                        use std::io::Write as _;
                        let _ = std::io::stdout().flush();
                    }
                }
            }

            let drain_deadline = Instant::now() + node_converge;
            while !clean_finalization_allowed(&pending, &pending_omissions) {
                let now_ms = profile_physical_now_ms();
                for (index, node) in ids.iter().enumerate() {
                    for key in pending[index].keys() {
                        let Some(expected) = reference.get(&key) else {
                            pending[index].healed(&key);
                            continue;
                        };
                        match profile_node_read_retry(&transport, *node, zone_id, &key, direct_retry).await {
                            Ok(got) => match check_expected(Api::Replica, &key, expected, got.as_ref(), now_ms, &model) {
                                Ok(_) => pending[index].healed(&key),
                                Err(Divergence::Hard(reason)) => panic!(
                                    "PROFILECHECK CORRECTNESS final debt readback node={node}: {reason}"
                                ),
                                Err(Divergence::Soft(reason)) => pending[index].note(&key, reason),
                            },
                            Err(e) => pending[index].note(&key, format!("read error {e:?}")),
                        }
                    }
                }
                let omissions: Vec<((u32, Vec<u8>), (Instant, vals_cluster::Version, Option<Vec<u8>>))> =
                    pending_omissions.iter().map(|(key, debt)| (key.clone(), debt.clone())).collect();
                for ((node, key), (_, version, value)) in omissions {
                    let expected = (version, value);
                    match profile_node_read_retry(&transport, node, zone_id, &key, direct_retry).await {
                        Ok(got) => match check_expected(Api::Replica, &key, &expected, got.as_ref(), now_ms, &model) {
                            Ok(_) => {
                                healed_live_cold_seed += 1;
                                pending_omissions.remove(&(node, key));
                            }
                            Err(Divergence::Hard(reason)) => panic!(
                                "PROFILECHECK CORRECTNESS final omission readback node={node}: {reason}"
                            ),
                            Err(Divergence::Soft(_)) => {}
                        },
                        Err(_) => {}
                    }
                }
                if clean_finalization_allowed(&pending, &pending_omissions) {
                    break;
                }
                let pending_divergences: usize = pending.iter().map(PendingDivergence::len).sum();
                for (index, node) in ids.iter().enumerate() {
                    if let Some((key, since, reason)) = pending[index].oldest() {
                        assert!(
                            since.elapsed() <= node_converge,
                            "node={node} key={:?} remained an unresolved final debt for {}s: {reason}",
                            String::from_utf8_lossy(&key),
                            since.elapsed().as_secs()
                        );
                    }
                }
                if let Some(((node, key), (since, _, _))) = pending_omissions.iter().min_by_key(|(_, debt)| debt.0) {
                    assert!(
                        since.elapsed() <= node_converge,
                        "node={node} key={:?} remained an unresolved named omission for {}s",
                        String::from_utf8_lossy(key),
                        since.elapsed().as_secs()
                    );
                }
                assert!(
                    Instant::now() < drain_deadline,
                    "profilecheck cannot finalize clean with {pending_divergences} pending replica divergences and {} named omissions",
                    pending_omissions.len()
                );
                eprintln!(
                    "PROFILECHECK FINAL_DEBT_WAIT pending_divergences={pending_divergences} pending_omissions={}",
                    pending_omissions.len()
                );
                profile_sleep(verify_poll).await;
            }
            if let Some(path) = &state_path {
                let header = clean_state_header(
                    round,
                    &versions_source,
                    &reference,
                    &pending,
                    &pending_omissions,
                );
                save_state(path, header, zone_id, &model, &reference);
            }
        },
        vals_cluster::reactor::poll_io,
    );
    if verify_only {
        println!(
            "PROFILECHECK VERIFY PASS seed=0x{PROFILE_SEED_IDENTITY:016x} nodes={} start_round={initial_round} keys={} verified_live={} verified_tombstones={} collected_tombstones={} rss_kib={} elapsed_s={:.3}",
            ids.len(), reference.len(), verify_counts.live, verify_counts.tombstones, verify_counts.collected,
            process_rss_kib(), started.elapsed().as_secs_f64(),
        );
    } else {
        println!(
            "PROFILECHECK PASS seed=0x{PROFILE_SEED_IDENTITY:016x} quorum={quorum} start_round={initial_round} rounds={} keys={} max_keys={} operations={operations} checkpoints={checkpoints} omitted_replica_writes={omitted_replica_writes} winner_tombstones={winner_tombstones} resurrections={resurrections} cold_overwrites={cold_overwrites} retired_keys={retired_keys} omitted_live_cold_seed={omitted_live_cold_seed} healed_live_cold_seed={healed_live_cold_seed} deferred_cold_overwrites={deferred_cold_overwrites} pending_omissions={} reconciled_winners={reconciled_winners} gc_grace_ms={} gc_probes={gc_probes} gc_collected={} gc_held={} gc_stale={} gc_unreadable={} rss_kib={} elapsed_s={:.3}",
            rounds, reference.len(), model.max_keys(), pending_omissions.len(), model.gc_grace_ms,
            gc_totals.collected, gc_totals.held, gc_totals.stale, gc_totals.unreadable, process_rss_kib(), started.elapsed().as_secs_f64(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_model() -> Model {
        Model { seed_keys: 8, workload_keys: 4, gc_grace_ms: 900_000, skew_margin_ms: 1_000 }
    }

    fn version(hlc: u64) -> vals_cluster::Version {
        vals_cluster::Version { hlc, node: PROFILE_WRITERS[2] }
    }

    #[test]
    fn profilecheck_scan_successor_pages_inclusive_bounds_without_repeats() {
        let keys: Vec<Vec<u8>> = (0..5001).map(|i| format!("key-{i:05}").into_bytes()).collect();
        let mut cursor: Option<Vec<u8>> = None;
        let mut seen = Vec::new();
        loop {
            let start = cursor.as_ref().map_or(0, |c| keys.partition_point(|k| k < c));
            let end = (start + 4096).min(keys.len());
            let page = &keys[start..end];
            seen.extend_from_slice(page);
            if end == keys.len() { break; }
            cursor = Some(scan_successor(page.last().unwrap().clone()));
        }
        assert_eq!(seen, keys);
    }

    #[test]
    fn profilecheck_state_is_versioned_and_bound_to_zone_seed_keyspace_and_grace() {
        let model = test_model();
        let mut reference = Reference::new();
        reference.insert(b"live".to_vec(), (vals_cluster::Version { hlc: 7, node: PROFILE_WRITERS[0] }, Some(b"v".to_vec())));
        reference.insert(b"dead".to_vec(), (vals_cluster::Version { hlc: 9, node: PROFILE_WRITERS[2] }, None));
        let header = StateHeader { round: 41, clean: true, high_hlc: 99 };
        let encoded = encode_state(header, 17, &model, &reference);
        assert_eq!(decode_state(&encoded, 17, &model).unwrap(), (header, reference));
        assert!(decode_state(&encoded, 18, &model).unwrap_err().contains("zone"));
        assert!(decode_state(&encoded, 17, &Model { seed_keys: 9, ..model }).unwrap_err().contains("seed_keys"));
        assert!(decode_state(&encoded, 17, &Model { workload_keys: 5, ..model }).unwrap_err().contains("workload_keys"));
        assert!(decode_state(&encoded, 17, &Model { gc_grace_ms: 60_000, ..model }).unwrap_err().contains("gc_grace_ms"));
        assert!(decode_state(&encoded[1..], 17, &model).unwrap_err().contains("magic"));
        assert!(decode_state(&encoded[..encoded.len() - 1], 17, &model).unwrap_err().contains("truncated"));
    }

    #[test]
    fn profilecheck_state_refuses_a_reference_above_the_bounded_keyspace() {
        let model = Model { seed_keys: 0, workload_keys: 1, gc_grace_ms: 1000, skew_margin_ms: 0 };
        let mut reference = Reference::new();
        for i in 0..8 {
            reference.insert(format!("k{i}").into_bytes(), (version(i + 1), Some(vec![1])));
        }
        let encoded = encode_state(StateHeader { round: 1, clean: true, high_hlc: 99 }, 3, &model, &reference);
        assert!(decode_state(&encoded, 3, &model).unwrap_err().contains("bounded model maximum"));
    }

    #[test]
    fn profilecheck_state_records_clean_shutdown_and_a_consistent_high_watermark() {
        let model = test_model();
        let mut reference = Reference::new();
        reference.insert(b"k".to_vec(), (version(500), Some(b"v".to_vec())));
        let dirty = encode_state(StateHeader { round: 3, clean: false, high_hlc: 700 }, 5, &model, &reference);
        let (header, _) = decode_state(&dirty, 5, &model).unwrap();
        assert!(!header.clean);
        assert_eq!(header.high_hlc, 700);
        let understated = encode_state(StateHeader { round: 3, clean: true, high_hlc: 1 }, 5, &model, &reference);
        assert!(decode_state(&understated, 5, &model).unwrap_err().contains("high_hlc"));
    }

    #[test]
    fn profilecheck_resumed_versions_beat_persisted_ones_under_a_stalled_or_backward_clock() {
        let model = test_model();
        let mut reference = Reference::new();
        reference.insert(b"a".to_vec(), (version(9_000 << PROFILE_COUNTER_BITS), Some(b"v".to_vec())));
        let _ = model;
        let mut resumed = VersionSource::resumed_from(0, &reference);
        let same_ms = resumed.next_triple(9_000);
        assert!(same_ms[0].hlc > (9_000 << PROFILE_COUNTER_BITS), "same-millisecond resume must advance the counter");
        let backwards = resumed.next_triple(1_000);
        assert!(backwards[0] > same_ms[2], "a backward wall clock must not reissue an old version");
        let mut from_header = VersionSource::resumed_from(10_000 << PROFILE_COUNTER_BITS, &reference);
        let next = from_header.next_triple(9_000);
        assert!(next[0].hlc > (10_000 << PROFILE_COUNTER_BITS), "persisted high water must dominate a lagging clock");
        assert_eq!(from_header.high_hlc(), next[2].hlc);
    }

    #[test]
    fn profilecheck_planned_acks_flag_only_unknown_higher_winners() {
        let planned = version(1_000);
        assert_eq!(classify_planned_ack(planned, &vals_cluster::Applied::Won).unwrap(), None);
        assert_eq!(
            classify_planned_ack(planned, &vals_cluster::Applied::Lost(version(1_000))).unwrap(),
            None,
            "an exact-retry loss to our own candidate is legal"
        );
        assert_eq!(
            classify_planned_ack(planned, &vals_cluster::Applied::Lost(version(999))).unwrap(),
            None,
            "a lower candidate losing to the planned maximum is legal"
        );
        assert_eq!(
            classify_planned_ack(planned, &vals_cluster::Applied::Lost(version(1_001))).unwrap(),
            Some(version(1_001)),
            "an unknown higher winner must be surfaced for fail-or-dirty-reconcile policy"
        );
        assert!(classify_planned_ack(planned, &vals_cluster::Applied::Rejected).is_err());
    }

    #[test]
    fn profilecheck_normal_mode_rejects_an_identical_higher_winner_without_bumping_state() {
        let planned = version(1_000);
        let observed = version(1_001);
        let mut source = VersionSource { last_hlc: 500 };
        for _ in 0..2 {
            let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                reconciliation_version(
                    &mut source,
                    false,
                    observed,
                    planned,
                    b"same-key",
                    7,
                    "unit",
                );
            }));
            assert!(failure.is_err(), "normal mode must reject the unknown winner");
            assert_eq!(source.high_hlc(), 500, "rejection must happen before creating a reconciling write version");
        }
    }

    #[test]
    fn profilecheck_reconciliation_requires_an_explicit_one_and_is_bounded() {
        use std::ffi::OsStr;

        assert!(!reconciliation_is_allowed(false, false, Some(OsStr::new("1"))), "fresh state is never a dirty resume");
        assert!(!reconciliation_is_allowed(true, true, Some(OsStr::new("1"))), "clean loaded state cannot reconcile");
        assert!(!reconciliation_is_allowed(true, false, None));
        assert!(!reconciliation_is_allowed(true, false, Some(OsStr::new("0"))), "environment presence with value zero must stay disabled");
        assert!(reconciliation_is_allowed(true, false, Some(OsStr::new("1"))));
        let model = test_model();
        let (empty_dirty, reference) = decode_state(
            &encode_state(StateHeader::default(), 5, &model, &Reference::new()),
            5,
            &model,
        ).unwrap();
        assert!(!empty_dirty.clean && empty_dirty.round == 0 && reference.is_empty());
        assert!(reconciliation_is_allowed(true, empty_dirty.clean, Some(OsStr::new("1"))));

        let mut source = VersionSource { last_hlc: 500 };
        let replacement = reconciliation_version(
            &mut source,
            true,
            version(1_001),
            version(1_000),
            b"dirty-key",
            8,
            "unit",
        );
        assert!(replacement > version(1_001));
        let mut reconciled = 0;
        record_reconciliations(&mut reconciled, 1, 1);
        let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            record_reconciliations(&mut reconciled, 1, 1);
        }));
        assert!(failure.is_err(), "the configured reconciliation bound must fail closed");
    }

    #[test]
    fn profilecheck_pending_divergence_keeps_failing_keys_until_they_heal() {
        let mut pending = PendingDivergence::default();
        pending.note(b"slow", "stale".into());
        let first = pending.oldest().expect("tracked").1;
        pending.note(b"other", "stale".into());
        pending.healed(b"other");
        assert_eq!(pending.len(), 1, "an unrelated clean key must not clear a pending one");
        pending.note(b"slow", "still stale".into());
        let (key, since, reason) = pending.oldest().expect("tracked");
        assert_eq!(key, b"slow".to_vec());
        assert_eq!(since, first, "the divergence clock must keep its first-seen instant");
        assert_eq!(reason, "still stale");
        pending.healed(b"slow");
        assert_eq!(pending.len(), 0);
        assert!(pending.oldest().is_none());
    }

    #[test]
    fn profilecheck_reconciliation_tracks_only_proved_destinations_and_keeps_debt_age() {
        let mut pending = vec![
            PendingDivergence::default(),
            PendingDivergence::default(),
            PendingDivergence::default(),
        ];
        pending[1].note(b"missed", "older divergence".into());
        let original = pending[1].oldest().expect("existing debt").1;
        pending[0].note(b"missed", "stale".into());
        pending[2].note(b"missed", "stale".into());

        track_reconciliation_destinations(&mut pending, b"missed", &[0, 2], &[1]);

        assert_eq!(pending[0].len(), 0);
        assert_eq!(pending[2].len(), 0);
        let (key, since, reason) = pending[1].oldest().expect("omitted destination remains owed");
        assert_eq!(key, b"missed".to_vec());
        assert_eq!(since, original, "an omitted reconciliation must retain the original debt timestamp");
        assert_eq!(reason, "reconciliation write was not acknowledged");
    }

    #[test]
    fn profilecheck_unresolved_debt_cannot_produce_a_clean_final_state() {
        let mut pending = vec![PendingDivergence::default()];
        let omissions = PendingOmissions::default();
        assert!(clean_finalization_allowed(&pending, &omissions));
        pending[0].note(b"owed", "missing readback".into());
        assert!(!clean_finalization_allowed(&pending, &omissions));
        let failure = std::panic::catch_unwind(|| {
            clean_state_header(3, &VersionSource::default(), &Reference::new(), &pending, &omissions)
        });
        assert!(failure.is_err(), "unresolved per-node debt must prevent creation of a clean header");

        let mut pending = vec![PendingDivergence::default()];
        let mut omissions = PendingOmissions::default();
        omissions.insert((1, b"owed".to_vec()), (Instant::now(), version(7), Some(b"v".to_vec())));
        assert!(!clean_finalization_allowed(&pending, &omissions));
        let failure = std::panic::catch_unwind(|| {
            clean_state_header(3, &VersionSource::default(), &Reference::new(), &pending, &omissions)
        });
        assert!(failure.is_err(), "a named omission must prevent creation of a clean header");
        pending[0].note(b"other", "stale".into());
        assert!(!clean_finalization_allowed(&pending, &omissions));
    }

    #[test]
    fn profilecheck_client_config_is_finite_and_validated() {
        let config = profile_client_config();
        assert!(config.connect_deadline.is_some_and(|d| !d.is_zero()));
        assert!(config.rpc_deadline.is_some_and(|d| !d.is_zero()));
        assert!(config.budgets.is_some(), "the driver must use the bounded default credit pool");
        assert_eq!(config.validate().unwrap(), config);
    }

    #[test]
    fn profilecheck_versions_increase_even_inside_one_millisecond() {
        let mut source = VersionSource::default();
        let first = source.next_triple(1_000);
        let second = source.next_triple(1_000);
        let third = source.next_triple(1_001);
        assert!(first[0] < first[1] && first[1] < first[2]);
        assert!(first[2] < second[0], "reused keys need strictly newer candidates");
        assert!(second[2] < third[0]);
        assert_eq!(first[0].physical_ms(), 1_000);
        assert_eq!(third[0].physical_ms(), 1_001);
        for version in first.iter().chain(second.iter()) {
            assert!(version.node > u32::MAX as u64);
        }
    }

    #[test]
    fn profilecheck_versions_track_wall_clock_for_gc_grace() {
        let before = profile_physical_now_ms();
        let versions = VersionSource::default().next_triple(profile_physical_now_ms());
        let after = profile_physical_now_ms();
        for version in versions {
            assert!((before..=after).contains(&version.physical_ms()));
        }
    }

    #[test]
    fn profilecheck_round_plans_stay_inside_a_bounded_keyspace() {
        let model = test_model();
        let mut keys = std::collections::BTreeSet::new();
        let mut retired = std::collections::BTreeSet::new();
        for round in 0..200 {
            let plan = round_plan(round, &model);
            keys.insert(plan.direct_key);
            keys.insert(plan.batch_key);
            keys.insert(plan.coordinator_key);
            keys.insert(plan.cas_key);
            if let Some(key) = plan.retired_key {
                assert!(round < model.workload_keys, "retired keys are written once and never rewritten");
                retired.insert(key.clone());
                keys.insert(key);
            }
            let (seed, _) = plan.seed_overwrite.expect("seeded model rewrites cold keys");
            assert!(seed.starts_with(b"seed-"));
        }
        assert_eq!(retired.len(), model.workload_keys);
        assert_eq!(keys.len(), model.workload_keys * 5);
        assert!(keys.len() + model.seed_keys <= model.max_keys());
    }

    #[test]
    fn profilecheck_generated_rounds_produce_winning_tombstones_and_resurrections() {
        let model = test_model();
        let mut latest: std::collections::BTreeMap<Vec<u8>, bool> = Default::default();
        let mut tombstones = 0;
        let mut resurrections = 0;
        for round in 0..200 {
            let plan = round_plan(round, &model);
            for (key, values) in [(plan.direct_key, plan.direct_values), (plan.batch_key, plan.batch_values)] {
                assert!(values[0].is_some(), "the oldest candidate must be a put");
                assert_ne!(values[1].is_some(), values[2].is_some(), "loser and winner kinds must differ");
                let live = values[2].is_some();
                if !live {
                    tombstones += 1;
                }
                if live && latest.get(&key) == Some(&false) {
                    resurrections += 1;
                }
                latest.insert(key, live);
            }
        }
        assert!(tombstones > 0, "generated rounds must include winning tombstones");
        assert!(resurrections > 0, "generated rounds must resurrect tombstoned keys");
    }

    #[test]
    fn profilecheck_generated_winner_cases_are_checked_against_replica_semantics() {
        let model = test_model();
        let now = 10_000_000;
        for round in 0..40 {
            let plan = round_plan(round, &model);
            let winner = version(now << PROFILE_COUNTER_BITS);
            let expected = (winner, plan.direct_values[2].clone());
            let stored = vals_cluster::Stored { version: winner, value: plan.direct_values[2].clone() };
            let counts = check_expected(Api::Replica, &plan.direct_key, &expected, Some(&stored), now, &model).unwrap();
            assert_eq!(counts.live + counts.tombstones, 1);
            let loser = vals_cluster::Stored { version: winner, value: plan.direct_values[0].clone() };
            if plan.direct_values[2] != plan.direct_values[0] {
                assert!(matches!(
                    check_expected(Api::Replica, &plan.direct_key, &expected, Some(&loser), now, &model),
                    Err(Divergence::Hard(_))
                ));
            }
            assert!(matches!(
                check_expected(Api::Replica, &plan.direct_key, &expected, None, now, &model),
                Err(Divergence::Soft(_))
            ));
        }
    }

    #[test]
    fn profilecheck_point_reads_hide_tombstones_but_never_values() {
        let model = test_model();
        let now = 1_000_000;
        let winner = version(now << PROFILE_COUNTER_BITS);
        let tombstone = (winner, None);
        assert_eq!(
            check_expected(Api::PointRead, b"k", &tombstone, None, now, &model).unwrap(),
            Counts { hidden: 1, ..Counts::default() }
        );
        let resurrected = vals_cluster::Stored { version: winner, value: Some(b"v".to_vec()) };
        assert!(matches!(
            check_expected(Api::PointRead, b"k", &tombstone, Some(&resurrected), now, &model),
            Err(Divergence::Hard(_))
        ));
        let live = (winner, Some(b"v".to_vec()));
        assert!(matches!(
            check_expected(Api::PointRead, b"k", &live, None, now, &model),
            Err(Divergence::Hard(_))
        ));
        let stale = vals_cluster::Stored { version: version((now - 1) << PROFILE_COUNTER_BITS), value: Some(b"old".to_vec()) };
        assert!(matches!(
            check_expected(Api::PointRead, b"k", &live, Some(&stale), now, &model),
            Err(Divergence::Hard(_))
        ));
    }

    #[test]
    fn profilecheck_replica_tombstones_are_exact_before_grace_and_collectable_after() {
        let model = test_model();
        let now = 2_000_000;
        let fresh = version((now - 1_000) << PROFILE_COUNTER_BITS);
        let fresh_tombstone = (fresh, None);
        assert_eq!(
            check_expected(Api::Replica, b"k", &fresh_tombstone, Some(&vals_cluster::Stored { version: fresh, value: None }), now, &model).unwrap(),
            Counts { tombstones: 1, ..Counts::default() }
        );
        assert!(matches!(
            check_expected(Api::Replica, b"k", &fresh_tombstone, None, now, &model),
            Err(Divergence::Soft(_))
        ));
        assert!(matches!(
            check_expected(Api::Replica, b"k", &fresh_tombstone, Some(&vals_cluster::Stored { version: fresh, value: Some(b"v".to_vec()) }), now, &model),
            Err(Divergence::Hard(_))
        ));
        let newer = vals_cluster::Stored { version: version(fresh.hlc + 1), value: Some(b"v".to_vec()) };
        assert!(matches!(
            check_expected(Api::Replica, b"k", &fresh_tombstone, Some(&newer), now, &model),
            Err(Divergence::Hard(_))
        ));
        let older = vals_cluster::Stored { version: version(fresh.hlc - 1), value: Some(b"v".to_vec()) };
        assert!(matches!(
            check_expected(Api::Replica, b"k", &fresh_tombstone, Some(&older), now, &model),
            Err(Divergence::Soft(_))
        ));
        let expired = version((now - model.gc_grace_ms - 5_000) << PROFILE_COUNTER_BITS);
        assert_eq!(
            check_expected(Api::Replica, b"k", &(expired, None), None, now, &model).unwrap(),
            Counts { collected: 1, ..Counts::default() }
        );
        assert_eq!(
            check_expected(Api::Replica, b"k", &(expired, None), Some(&vals_cluster::Stored { version: expired, value: None }), now, &model).unwrap(),
            Counts { tombstones: 1, ..Counts::default() }
        );
        assert!(matches!(
            check_expected(Api::Replica, b"k", &(expired, None), Some(&vals_cluster::Stored { version: expired, value: Some(b"v".to_vec()) }), now, &model),
            Err(Divergence::Hard(_))
        ));
    }

    #[test]
    fn profilecheck_gc_probe_fails_fast_on_uncollected_overdue_tombstones() {
        let model = Model { gc_grace_ms: 10_000, skew_margin_ms: 1_000, ..test_model() };
        let now = 100_000;
        assert!(!model.tombstone_is_overdue(version((now - 15_000) << PROFILE_COUNTER_BITS), now, 5_000));
        assert!(model.tombstone_is_overdue(version((now - 16_000) << PROFILE_COUNTER_BITS), now, 5_000));
        assert!(GcProbe::default().verdict(10).is_ok());
        assert!(GcProbe { stale: 3, unreadable: 9, ..GcProbe::default() }.verdict(10).is_ok());
        assert!(GcProbe { held: 4, ..GcProbe::default() }.verdict(100).is_err());
        assert!(GcProbe { collected: 9, held: 1, ..GcProbe::default() }.verdict(10).is_ok());
        assert!(GcProbe { collected: 8, held: 2, ..GcProbe::default() }.verdict(10).is_err());
        assert!(GcProbe { collected: 1, held: 9, ..GcProbe::default() }.verdict(100).is_ok());
    }

    #[test]
    fn profilecheck_grace_boundary_uses_a_clock_skew_margin() {
        let model = Model { gc_grace_ms: 10_000, skew_margin_ms: 1_000, ..test_model() };
        let now = 100_000;
        assert!(model.tombstone_must_be_present(version((now - 8_000) << PROFILE_COUNTER_BITS), now));
        assert!(!model.tombstone_must_be_present(version((now - 9_500) << PROFILE_COUNTER_BITS), now));
        assert!(!model.tombstone_must_be_present(version((now - 20_000) << PROFILE_COUNTER_BITS), now));
    }

    #[test]
    fn profilecheck_live_keys_reject_value_and_version_drift() {
        let model = test_model();
        let now = 3_000_000;
        let winner = version(now << PROFILE_COUNTER_BITS);
        let live = (winner, Some(b"v".to_vec()));
        assert!(matches!(
            check_expected(Api::Replica, b"k", &live, Some(&vals_cluster::Stored { version: winner, value: Some(b"other".to_vec()) }), now, &model),
            Err(Divergence::Hard(_))
        ));
        assert!(matches!(
            check_expected(Api::Replica, b"k", &live, Some(&vals_cluster::Stored { version: version(winner.hlc + 1), value: Some(b"v".to_vec()) }), now, &model),
            Err(Divergence::Hard(_))
        ));
        assert!(matches!(
            check_expected(Api::Replica, b"k", &live, Some(&vals_cluster::Stored { version: version(winner.hlc - 1), value: Some(b"v".to_vec()) }), now, &model),
            Err(Divergence::Soft(_))
        ));
        assert!(matches!(
            check_expected(Api::Replica, b"k", &live, None, now, &model),
            Err(Divergence::Soft(_))
        ));
    }

    #[test]
    fn profilecheck_ambiguous_cas_requires_safe_readback() {
        let expected = vals_cluster::Version { hlc: 10, node: PROFILE_WRITERS[0] };
        let applied = vals_cluster::Stored {
            version: vals_cluster::Version { hlc: 11, node: PROFILE_WRITERS[1] },
            value: Some(b"desired".to_vec()),
        };
        assert!(matches!(
            classify_ambiguous_cas(expected, b"desired", Some(&applied)).unwrap(),
            CasReadback::Applied(version) if version == applied.version
        ));
        let unchanged = vals_cluster::Stored { version: expected, value: Some(b"old".to_vec()) };
        assert!(matches!(
            classify_ambiguous_cas(expected, b"desired", Some(&unchanged)).unwrap(),
            CasReadback::Retry
        ));
        assert!(classify_ambiguous_cas(expected, b"desired", None).is_err());
        let conflict = vals_cluster::Stored {
            version: vals_cluster::Version { hlc: 12, node: PROFILE_WRITERS[2] },
            value: Some(b"third-state".to_vec()),
        };
        assert!(classify_ambiguous_cas(expected, b"desired", Some(&conflict)).is_err());
    }
}
