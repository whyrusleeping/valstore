use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use vals_cluster::NodeId;
use vals_cluster::Ordering;

#[derive(Debug)]
pub struct ConfigError {
    pub line: usize,
    pub msg: String,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.line == 0 {
            write!(f, "{}", self.msg)
        } else {
            write!(f, "line {}: {}", self.line, self.msg)
        }
    }
}

impl std::error::Error for ConfigError {}

fn err(line: usize, msg: impl Into<String>) -> ConfigError {
    ConfigError {
        line,
        msg: msg.into(),
    }
}

struct Raw {
    entries: BTreeMap<String, Vec<(usize, String)>>,
}

impl Raw {
    fn parse(text: &str) -> Result<Raw, ConfigError> {
        let mut entries: BTreeMap<String, Vec<(usize, String)>> = BTreeMap::new();
        for (i, raw_line) in text.lines().enumerate() {
            let line_no = i + 1;
            let line = match raw_line.find('#') {
                Some(p) => &raw_line[..p],
                None => raw_line,
            }
            .trim();
            if line.is_empty() {
                continue;
            }
            let Some((key, value)) = line.split_once('=') else {
                return Err(err(
                    line_no,
                    format!("expected `key = value`, got {line:?}"),
                ));
            };
            let key = key.trim();
            let value = value.trim();
            if key.is_empty() {
                return Err(err(line_no, "empty key"));
            }
            entries
                .entry(key.to_string())
                .or_default()
                .push((line_no, value.to_string()));
        }
        Ok(Raw { entries })
    }

    fn take_one(&mut self, key: &str) -> Result<Option<(usize, String)>, ConfigError> {
        match self.entries.remove(key) {
            None => Ok(None),
            Some(mut v) if v.len() == 1 => Ok(Some(v.pop().unwrap())),
            Some(v) => Err(err(
                v[1].0,
                format!("`{key}` given more than once (first at line {})", v[0].0),
            )),
        }
    }

    fn take_all(&mut self, key: &str) -> Vec<(usize, String)> {
        self.entries.remove(key).unwrap_or_default()
    }

    fn finish(self) -> Result<(), ConfigError> {
        if let Some((key, occ)) = self.entries.into_iter().next() {
            return Err(err(occ[0].0, format!("unknown key `{key}`")));
        }
        Ok(())
    }
}

fn parse_int<T: std::str::FromStr>(line: usize, key: &str, v: &str) -> Result<T, ConfigError> {
    v.parse()
        .map_err(|_| err(line, format!("`{key}`: not a number: {v:?}")))
}

fn take_int<T: std::str::FromStr>(
    raw: &mut Raw,
    key: &str,
    default: T,
) -> Result<T, ConfigError> {
    match raw.take_one(key)? {
        Some((line, value)) => parse_int(line, key, &value),
        None => Ok(default),
    }
}

fn parse_zone_region_override(
    line: usize,
    key: &str,
    v: &str,
) -> Result<(u32, u64), ConfigError> {
    let Some((zone_id, mib)) = v.split_once(':') else {
        return Err(err(
            line,
            format!("`{key}`: expected `<zone-id>:<MiB>`, got {v:?}"),
        ));
    };
    let zone_id = parse_int(line, key, zone_id.trim())?;
    let mib = parse_int(line, key, mib.trim())?;
    Ok((zone_id, mib))
}

fn parse_bool(line: usize, key: &str, v: &str) -> Result<bool, ConfigError> {
    match v {
        "true" | "yes" | "on" => Ok(true),
        "false" | "no" | "off" => Ok(false),
        _ => Err(err(
            line,
            format!("`{key}`: expected true/false, got {v:?}"),
        )),
    }
}

fn parse_duration(line: usize, key: &str, v: &str) -> Result<Duration, ConfigError> {
    let (num, unit) = if let Some(n) = v.strip_suffix("ms") {
        (n, 1u64)
    } else if let Some(n) = v.strip_suffix('s') {
        (n, 1000)
    } else {
        return Err(err(
            line,
            format!("`{key}`: durations need an `s` or `ms` suffix, got {v:?}"),
        ));
    };
    let n: u64 = parse_int(line, key, num.trim())?;
    let millis = n
        .checked_mul(unit)
        .ok_or_else(|| err(line, format!("`{key}`: duration is too large")))?;
    Ok(Duration::from_millis(millis))
}

fn parse_addr(line: usize, key: &str, v: &str) -> Result<SocketAddr, ConfigError> {
    v.parse()
        .map_err(|_| err(line, format!("`{key}`: not a socket address: {v:?}")))
}

fn parse_node_addrs(
    line: usize,
    key: &str,
    v: &str,
) -> Result<(NodeId, Vec<SocketAddr>), ConfigError> {
    let Some((id, addrs)) = v.split_once('@') else {
        return Err(err(
            line,
            format!("`{key}`: expected `<id> @ <addr>[,<addr>...]`, got {v:?}"),
        ));
    };
    let id: NodeId = parse_int(line, key, id.trim())?;
    let addrs = addrs
        .split(',')
        .map(|a| parse_addr(line, key, a.trim()))
        .collect::<Result<Vec<_>, _>>()?;
    if addrs.is_empty() {
        return Err(err(line, format!("`{key}`: no addresses for node {id}")));
    }
    Ok((id, addrs))
}

fn parse_data_node(
    line: usize,
    key: &str,
    v: &str,
) -> Result<(NodeId, u32, Vec<SocketAddr>), ConfigError> {
    let Some((lhs, addrs)) = v.split_once('@') else {
        return Err(err(
            line,
            format!("`{key}`: expected `<id>[*<weight>] @ <addr>[,<addr>...]`, got {v:?}"),
        ));
    };
    let (id_str, weight) = match lhs.split_once('*') {
        Some((id, w)) => {
            let weight: u32 = parse_int(line, key, w.trim())?;
            if weight == 0 {
                return Err(err(line, format!("`{key}`: weight must be >= 1, got 0")));
            }
            (id, weight)
        }
        None => (lhs, 1),
    };
    let id: NodeId = parse_int(line, key, id_str.trim())?;
    let addrs = addrs
        .split(',')
        .map(|a| parse_addr(line, key, a.trim()))
        .collect::<Result<Vec<_>, _>>()?;
    if addrs.is_empty() {
        return Err(err(line, format!("`{key}`: no addresses for node {id}")));
    }
    Ok((id, weight, addrs))
}

fn parse_ordering(line: usize, key: &str, v: &str) -> Result<Ordering, ConfigError> {
    match v {
        "sorted" => Ok(Ordering::Sorted),
        "unsorted" => Ok(Ordering::Unsorted),
        _ => Err(err(
            line,
            format!("`{key}`: expected sorted|unsorted, got {v:?}"),
        )),
    }
}

pub fn resolve_log_level(configured: Option<log::LevelFilter>) -> Result<(log::LevelFilter, bool), ConfigError> {
    let legacy = match std::env::var("VALS_LOG") {
        Ok(value) => Some(parse_log_level(0, "VALS_LOG (config key `log_level`)", &value)?),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => return Err(err(0, format!("VALS_LOG: {error}"))),
    };
    match (configured, legacy) {
        (Some(config), Some(env)) if config != env => Err(err(0, format!("conflicting `log_level`={config} and VALS_LOG={env}; remove the legacy environment variable or make both values equal"))),
        (Some(config), _) => Ok((config, false)),
        (None, Some(env)) => Ok((env, true)),
        (None, None) => Ok((log::LevelFilter::Info, false)),
    }
}

fn parse_log_level(line: usize, key: &str, v: &str) -> Result<log::LevelFilter, ConfigError> {
    match v {
        "error" => Ok(log::LevelFilter::Error),
        "warn" => Ok(log::LevelFilter::Warn),
        "info" => Ok(log::LevelFilter::Info),
        "debug" => Ok(log::LevelFilter::Debug),
        "trace" => Ok(log::LevelFilter::Trace),
        "off" => Ok(log::LevelFilter::Off),
        _ => Err(err(
            line,
            format!("`{key}`: expected error|warn|info|debug|trace|off, got {v:?}"),
        )),
    }
}

fn take_secret(raw: &mut Raw, dir: &Path) -> Result<Option<Vec<u8>>, ConfigError> {
    let lit = raw.take_one("secret")?;
    let file = raw.take_one("secret_file")?;
    match (lit, file) {
        (Some(_), Some((line, _))) => Err(err(line, "give `secret` or `secret_file`, not both")),
        (Some((_, s)), None) => Ok(Some(s.into_bytes())),
        (None, Some((line, p))) => {
            let path = dir.join(&p);
            let bytes = std::fs::read(&path)
                .map_err(|e| err(line, format!("`secret_file`: {}: {e}", path.display())))?;
            let trimmed = bytes
                .strip_suffix(b"\n")
                .map(|b| b.to_vec())
                .unwrap_or(bytes);
            Ok(Some(trimmed))
        }
        (None, None) => Ok(None),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    File,
    Mem,
    Spdk,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    Capacity,
    Latency,
}

impl Profile {
    pub fn blind_append(self) -> bool {
        self == Profile::Latency
    }

    pub fn winner_directory(self) -> bool {
        self == Profile::Latency
    }
}

impl fmt::Display for Profile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Profile::Capacity => f.write_str("capacity"),
            Profile::Latency => f.write_str("latency"),
        }
    }
}

#[derive(Debug)]
pub struct NodeConfig {
    pub settings: vals_cluster::node_settings::NodeSettingsOverrides,
    pub node_id: NodeId,
    pub profile: Profile,
    pub listen: Vec<SocketAddr>,
    pub backend: Backend,
    pub data_dir: Option<PathBuf>,
    pub peers: Vec<(NodeId, Vec<SocketAddr>)>,
    pub rf: usize,
    pub sweep: Duration,
    pub gc_grace: Duration,
    pub secret: Option<Vec<u8>>,
    pub block_size: usize,
    pub seg_blocks: u64,
    pub region_mib: u64,
    pub zone_region_mib: u64,
    pub zone_region_mib_overrides: Vec<(u32, u64)>,
    pub pci: Vec<String>,
    pub spdk_shm_id: i32,
    pub spdk_mem_mb: i32,
    pub spdk_core: i32,
    pub metrics: Option<SocketAddr>,
    pub admin_socket: Option<PathBuf>,
    pub log_level: Option<log::LevelFilter>,
}

impl NodeConfig {
    pub fn load(path: &Path) -> Result<NodeConfig, ConfigError> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| err(0, format!("{}: {e}", path.display())))?;
        Self::parse(&text, path.parent().unwrap_or(Path::new(".")))
    }

    pub(crate) fn parse(text: &str, dir: &Path) -> Result<NodeConfig, ConfigError> {
        let mut raw = Raw::parse(text)?;

        let (l, v) = raw
            .take_one("node_id")?
            .ok_or_else(|| err(0, "missing required key `node_id`"))?;
        let node_id = parse_int(l, "node_id", &v)?;

        let profile = match raw.take_one("profile")? {
            None => Profile::Capacity,
            Some((_, v)) if v == "capacity" => Profile::Capacity,
            Some((_, v)) if v == "latency" => Profile::Latency,
            Some((l, v)) => {
                return Err(err(
                    l,
                    format!("`profile`: expected capacity|latency, got {v:?}"),
                ));
            }
        };

        let listen = raw
            .take_all("listen")
            .into_iter()
            .map(|(l, v)| parse_addr(l, "listen", &v))
            .collect::<Result<Vec<_>, _>>()?;
        if listen.is_empty() {
            return Err(err(0, "missing required key `listen`"));
        }

        let backend = match raw.take_one("backend")? {
            None => Backend::File,
            Some((l, v)) => match v.as_str() {
                "file" => Backend::File,
                "mem" => Backend::Mem,
                "spdk" => Backend::Spdk,
                _ => return Err(err(l, format!("`backend`: file|mem|spdk, got {v:?}"))),
            },
        };

        let data_dir = raw.take_one("data_dir")?.map(|(_, v)| dir.join(v));
        let peers = raw
            .take_all("peer")
            .into_iter()
            .map(|(l, v)| parse_node_addrs(l, "peer", &v))
            .collect::<Result<Vec<_>, _>>()?;

        let rf = take_int(&mut raw, "rf", 3)?;
        let sweep = match raw.take_one("sweep")? {
            Some((l, v)) => parse_duration(l, "sweep", &v)?,
            None => Duration::from_secs(5),
        };
        let gc_grace = match raw.take_one("gc_grace")? {
            Some((l, v)) => parse_duration(l, "gc_grace", &v)?,
            None => Duration::ZERO,
        };
        let secret = take_secret(&mut raw, dir)?;
        let block_size = take_int(&mut raw, "block_size", 4096)?;
        let seg_blocks = take_int(&mut raw, "seg_blocks", 256)?;
        let region_mib: u64 = take_int(&mut raw, "region_mib", 1024)?;
        let zone_region_mib: u64 =
            take_int(&mut raw, "zone_region_mib", (region_mib / 8).max(1))?;
        let mut zone_region_mib_overrides = Vec::new();
        let mut override_lines = BTreeMap::new();
        for (l, v) in raw.take_all("zone_region_mib_override") {
            let (zone_id, mib) =
                parse_zone_region_override(l, "zone_region_mib_override", &v)?;
            if let Some(first) = override_lines.insert(zone_id, l) {
                return Err(err(
                    l,
                    format!(
                        "`zone_region_mib_override`: duplicate zone id {zone_id} \
                         (first at line {first})"
                    ),
                ));
            }
            zone_region_mib_overrides.push((zone_id, mib));
        }
        let pci = raw
            .take_all("pci")
            .into_iter()
            .map(|(_, v)| v)
            .collect::<Vec<_>>();
        if let Some((l, v)) = raw.take_one("format")? {
            if parse_bool(l, "format", &v)? {
                return Err(err(
                    l,
                    "`format = true` is no longer accepted at daemon startup; remove it and run \
                     `valsd provision` while the node is offline",
                ));
            }
        }
        let spdk_shm_id = take_int(&mut raw, "spdk_shm_id", node_id as i32)?;
        let spdk_mem_mb = take_int(&mut raw, "spdk_mem_mb", 0)?;
        let spdk_core = take_int(&mut raw, "spdk_core", -1)?;
        let metrics = match raw.take_one("metrics")? {
            Some((l, v)) => Some(parse_addr(l, "metrics", &v)?),
            None => None,
        };
        let admin_socket = raw.take_one("admin_socket")?.map(|(_, v)| dir.join(v));
        let log_level = match raw.take_one("log_level")? {
            Some((l, v)) => Some(parse_log_level(l, "log_level", &v)?),
            None => None,
        };
        let mut settings = vals_cluster::node_settings::NodeSettingsOverrides::default();
        for key in vals_cluster::node_settings::NodeSettingsOverrides::KEYS {
            if let Some((line, value)) = raw.take_one(key)? {
                settings.set(key, &value).map_err(|error| err(line, error))?;
            }
        }
        settings.validate().map_err(|error| err(0, error))?;
        raw.finish()?;
        match backend {
            Backend::File if data_dir.is_none() => {
                return Err(err(0, "backend = file requires `data_dir`"));
            }
            Backend::File | Backend::Mem if listen.len() != 1 => {
                return Err(err(
                    0,
                    "file/mem backends serve unsharded: give exactly one `listen`",
                ));
            }
            Backend::Spdk if pci.is_empty() => {
                return Err(err(0, "backend = spdk requires at least one `pci`"));
            }
            Backend::Spdk if block_size != 4096 => {
                return Err(err(0, "backend = spdk requires `block_size = 4096`"));
            }
            _ => {}
        }
        if rf == 0 {
            return Err(err(0, "`rf` must be >= 1"));
        }
        u32::try_from(rf).map_err(|_| err(0, "`rf` exceeds the wire-format limit"))?;
        if sweep.is_zero() {
            return Err(err(
                0,
                "`sweep` must be greater than zero; anti-entropy cannot be disabled by cadence",
            ));
        }
        if gc_grace.as_millis() > u64::MAX as u128 {
            return Err(err(0, "`gc_grace` is too large to represent in milliseconds"));
        }
        if block_size == 0 {
            return Err(err(0, "`block_size` must be >= 1"));
        }
        if seg_blocks == 0 {
            return Err(err(0, "`seg_blocks` must be >= 1"));
        }
        let region_bytes = region_mib
            .checked_mul(1024 * 1024)
            .ok_or_else(|| err(0, "`region_mib` is too large to convert to bytes"))?;
        let block_size_u64 = u64::try_from(block_size)
            .map_err(|_| err(0, "`block_size` is too large for device geometry"))?;
        if region_bytes == 0 {
            return Err(err(0, "`region_mib` must be >= 1"));
        }
        if !region_bytes.is_multiple_of(block_size_u64) {
            return Err(err(0, "`region_mib` must contain a whole number of blocks"));
        }
        let region_blocks = region_bytes / block_size_u64;
        if region_blocks < seg_blocks {
            return Err(err(0, "`region_mib` must hold at least one segment"));
        }
        let segment_bytes = block_size_u64
            .checked_mul(seg_blocks)
            .ok_or_else(|| err(0, "`block_size * seg_blocks` overflows byte geometry"))?;
        if segment_bytes > u32::MAX as u64 {
            return Err(err(
                0,
                "`block_size * seg_blocks` exceeds the packed segment-offset limit",
            ));
        }
        if region_blocks / seg_blocks > u32::MAX as u64 + 1 {
            return Err(err(
                0,
                "`region_mib / seg_blocks` exceeds the packed segment-index limit",
            ));
        }

        let mut member_ids = BTreeSet::new();
        member_ids.insert(node_id);
        for (peer, _) in &peers {
            if !member_ids.insert(*peer) {
                return Err(err(
                    0,
                    format!("duplicate node id {peer} in `peer` membership"),
                ));
            }
        }
        if rf > member_ids.len() {
            return Err(err(
                0,
                format!("`rf` {rf} exceeds boot membership of {}", member_ids.len()),
            ));
        }

        let jbod = backend == Backend::Spdk && (pci.len() > 1 || listen.len() > 1);
        if !zone_region_mib_overrides.is_empty() && !jbod {
            return Err(err(
                0,
                "`zone_region_mib_override` is supported only on the sharded/JBOD SPDK path",
            ));
        }
        if jbod {
            if zone_region_mib == 0 {
                return Err(err(0, "`zone_region_mib` must be >= 1"));
            }
            if seg_blocks == 0 {
                return Err(err(0, "`seg_blocks` must be >= 1"));
            }
            let mib_blocks = 1024u64 * 1024 / 4096;
            let region_blocks = region_mib.checked_mul(mib_blocks).ok_or_else(|| {
                err(0, "`region_mib` is too large to convert to device blocks")
            })?;
            let fallback_blocks = zone_region_mib.checked_mul(mib_blocks).ok_or_else(|| {
                err(0, "`zone_region_mib` is too large to convert to device blocks")
            })?;
            let validate_zone_blocks = |key: &str, blocks: u64| -> Result<(), ConfigError> {
                if blocks < seg_blocks {
                    return Err(err(
                        0,
                        format!("`{key}` must hold at least one segment ({seg_blocks} blocks)"),
                    ));
                }
                if !blocks.is_multiple_of(seg_blocks) {
                    return Err(err(
                        0,
                        format!("`{key}` must be segment-aligned ({seg_blocks} blocks/segment)"),
                    ));
                }
                if 16u64.checked_add(blocks).is_none_or(|end| end > region_blocks) {
                    return Err(err(
                        0,
                        format!(
                            "`{key}` (+ the per-partition zone superblock) must fit within \
                             `region_mib`"
                        ),
                    ));
                }
                Ok(())
            };
            validate_zone_blocks("zone_region_mib", fallback_blocks)?;
            for &(zone_id, mib) in &zone_region_mib_overrides {
                if mib == 0 {
                    return Err(err(
                        0,
                        format!("`zone_region_mib_override` for zone id {zone_id} must be >= 1"),
                    ));
                }
                let blocks = mib.checked_mul(mib_blocks).ok_or_else(|| {
                    err(
                        0,
                        format!(
                            "`zone_region_mib_override` for zone id {zone_id} is too large"
                        ),
                    )
                })?;
                validate_zone_blocks("zone_region_mib_override", blocks)?;
            }
        }

        Ok(NodeConfig {
            settings,
            node_id,
            profile,
            listen,
            backend,
            data_dir,
            peers,
            rf,
            sweep,
            gc_grace,
            secret,
            block_size,
            seg_blocks,
            region_mib,
            zone_region_mib,
            zone_region_mib_overrides,
            pci,
            spdk_shm_id,
            spdk_mem_mb,
            spdk_core,
            metrics,
            admin_socket,
            log_level,
        })
    }
}

#[derive(Debug)]
pub struct BootZone {
    pub name: String,
    pub rf: usize,
    pub ordering: Ordering,
}

#[derive(Debug)]
pub struct PdConfig {
    pub pd_id: u64,
    pub data_dir: PathBuf,
    pub listen: Option<SocketAddr>,
    pub pd_peers: Vec<(u64, SocketAddr)>,
    pub campaign: bool,
    pub join: bool,
    pub data_nodes: Vec<(NodeId, u32, Vec<SocketAddr>)>,
    pub rf: u32,
    pub vnodes: u32,
    pub state_capacity_mib: usize,
    pub placement: vals_cluster::pd::PdPlacementOverrides,
    pub zones: Vec<BootZone>,
    pub admin_socket: PathBuf,
    pub secret: Option<Vec<u8>>,
    pub metrics: Option<SocketAddr>,
    pub log_level: Option<log::LevelFilter>,
}

impl PdConfig {
    pub fn load(path: &Path) -> Result<PdConfig, ConfigError> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| err(0, format!("{}: {e}", path.display())))?;
        Self::parse(&text, path.parent().unwrap_or(Path::new(".")))
    }

    fn parse(text: &str, dir: &Path) -> Result<PdConfig, ConfigError> {
        let mut raw = Raw::parse(text)?;

        let (l, v) = raw
            .take_one("pd_id")?
            .ok_or_else(|| err(0, "missing required key `pd_id`"))?;
        let pd_id = parse_int(l, "pd_id", &v)?;

        let data_dir = raw
            .take_one("data_dir")?
            .map(|(_, v)| dir.join(v))
            .ok_or_else(|| err(0, "missing required key `data_dir`"))?;

        let listen = match raw.take_one("listen")? {
            Some((l, v)) => Some(parse_addr(l, "listen", &v)?),
            None => None,
        };
        let pd_peers = raw
            .take_all("pd_peer")
            .into_iter()
            .map(|(l, v)| {
                let (id, addrs) = parse_node_addrs(l, "pd_peer", &v)?;
                if addrs.len() != 1 {
                    return Err(err(l, "`pd_peer`: exactly one address per PD replica"));
                }
                Ok((id as u64, addrs[0]))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let campaign = match raw.take_one("campaign")? {
            Some((l, v)) => parse_bool(l, "campaign", &v)?,
            None => false,
        };
        let join = match raw.take_one("join")? {
            Some((l, v)) => parse_bool(l, "join", &v)?,
            None => false,
        };
        let data_nodes = raw
            .take_all("data_node")
            .into_iter()
            .map(|(l, v)| parse_data_node(l, "data_node", &v))
            .collect::<Result<Vec<_>, _>>()?;
        let rf = take_int(&mut raw, "rf", 3)?;
        let vnodes = take_int(&mut raw, "vnodes", vals_cluster::ring::DEFAULT_VNODES)?;
        let state_capacity_mib = take_int(&mut raw, "state_capacity_mib", 256usize)?;
        let mut placement = vals_cluster::pd::PdPlacementOverrides::default();
        let mut placement_line = 0;
        for key in vals_cluster::pd::PdPlacementOverrides::KEYS {
            if let Some((line, value)) = raw.take_one(key)? {
                placement.set(key, &value).map_err(|error| err(line, error))?;
                if matches!(*key, "balance_high_pct" | "balance_low_pct") {
                    placement_line = placement_line.max(line);
                }
            }
        }
        placement.validate().map_err(|error| err(placement_line, error))?;
        let zones = raw
            .take_all("zone")
            .into_iter()
            .map(|(l, v)| {
                let parts: Vec<&str> = v.split_whitespace().collect();
                let [name, rf, ordering] = parts.as_slice() else {
                    return Err(err(
                        l,
                        format!("`zone`: expected `<name> <rf> <sorted|unsorted>`, got {v:?}"),
                    ));
                };
                Ok(BootZone {
                    name: name.to_string(),
                    rf: parse_int(l, "zone", rf)?,
                    ordering: parse_ordering(l, "zone", ordering)?,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let admin_socket = raw
            .take_one("admin_socket")?
            .map(|(_, v)| dir.join(v))
            .unwrap_or_else(|| data_dir.join("admin.sock"));
        let secret = take_secret(&mut raw, dir)?;
        let metrics = match raw.take_one("metrics")? {
            Some((l, v)) => Some(parse_addr(l, "metrics", &v)?),
            None => None,
        };
        let log_level = match raw.take_one("log_level")? {
            Some((l, v)) => Some(parse_log_level(l, "log_level", &v)?),
            None => None,
        };
        raw.finish()?;

        if !pd_peers.is_empty() && listen.is_none() {
            return Err(err(0, "multi-PD (`pd_peer` given) requires `listen`"));
        }
        if join && (pd_peers.is_empty() || campaign) {
            return Err(err(
                0,
                "`join = true` needs the existing group's `pd_peer` list and excludes `campaign`",
            ));
        }
        if rf == 0 {
            return Err(err(0, "`rf` must be >= 1"));
        }
        if state_capacity_mib == 0 || state_capacity_mib.checked_mul(1024 * 1024).is_none() {
            return Err(err(0, "`state_capacity_mib` must be a nonzero representable byte size"));
        }
        if vnodes == 0 {
            return Err(err(0, "`vnodes` must be >= 1"));
        }
        let mut pd_ids = BTreeSet::new();
        for (id, _) in &pd_peers {
            if !pd_ids.insert(*id) {
                return Err(err(0, format!("duplicate PD id {id} in `pd_peer`")));
            }
        }
        let mut member_ids = BTreeSet::new();
        for (id, _, _) in &data_nodes {
            if !member_ids.insert(*id) {
                return Err(err(0, format!("duplicate node id {id} in `data_node`")));
            }
        }
        if rf as usize > member_ids.len() {
            return Err(err(
                0,
                format!(
                    "`rf` {rf} exceeds configured data-node membership of {}",
                    member_ids.len()
                ),
            ));
        }
        let mut zone_names = BTreeSet::new();
        for zone in &zones {
            if !zone_names.insert(zone.name.clone()) {
                return Err(err(0, format!("duplicate zone name {:?}", zone.name)));
            }
            vals_cluster::ZoneConfig {
                name: zone.name.clone(),
                rf: zone.rf,
                ordering: zone.ordering,
                scope: None,
            }
            .validate(&member_ids)
            .map_err(|error| err(0, format!("zone {:?}: {error}", zone.name)))?;
        }

        Ok(PdConfig {
            pd_id,
            data_dir,
            listen,
            pd_peers,
            campaign,
            join,
            data_nodes,
            rf,
            vnodes,
            state_capacity_mib,
            placement,
            zones,
            admin_socket,
            secret,
            metrics,
            log_level,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_dealloc_limit_parses_and_zero_disables() {
        let base = "node_id=1\nlisten=127.0.0.1:1\nbackend=mem\nrf=1\n";
        for limit in [0, 256, u32::MAX] {
            let cfg = NodeConfig::parse(&format!("{base}device_dealloc_segments_per_tick={limit}\n"), Path::new(".")).unwrap();
            assert_eq!(cfg.settings.resolve(|_| None).unwrap().0.device_dealloc_segments_per_tick, limit);
        }
        for value in ["-1", "bad", "4294967296"] {
            assert!(NodeConfig::parse(&format!("{base}device_dealloc_segments_per_tick={value}\n"), Path::new(".")).is_err());
        }
    }

    #[test]
    fn pd_placement_keys_parse_and_validate_with_line_numbers() {
        let base = "pd_id=1\ndata_dir=pd\ndata_node=1@127.0.0.1:1\nrf=1\n";
        let c = PdConfig::parse(&format!("{base}range_split_threshold=42\nrehome_enabled=no\nopaque_range_maps=yes\nbalance_execute=1\nbalance_high_pct=130\nbalance_low_pct=110\n"), Path::new(".")).unwrap();
        let settings = c.placement.resolve(|_| None).unwrap().0;
        assert_eq!(settings.range_split_threshold, 42);
        assert!(!settings.rehome_enabled);
        assert!(settings.opaque_range_maps);
        assert!(settings.balance_execute);
        assert_eq!(settings.balance_high_pct, 130);
        assert_eq!(settings.balance_low_pct, 110);
        for setting in [
            "rehome_enabled=maybe", "range_split_threshold=-1",
            "range_split_poll_ms=0", "range_splits_per_poll=0",
            "max_concurrent_rehomes=0", "balance_high_pct=99",
            "balance_low_pct=99", "balance_token_search_limit=0",
        ] {
            let error = PdConfig::parse(&format!("{base}{setting}"), Path::new(".")).unwrap_err();
            assert_eq!(error.line, 5, "{error}");
            assert!(error.msg.contains(setting.split('=').next().unwrap()), "{error}");
        }
        let error = PdConfig::parse(&format!("{base}balance_low_pct=130\nbalance_high_pct=120"), Path::new(".")).unwrap_err();
        assert_eq!(error.line, 6);
        assert!(error.msg.contains("balance_low_pct"));
    }

    #[test]
    fn node_config_parses_with_defaults_and_repeats() {
        let text = "
            node_id = 3            # trailing comment
            listen  = 0.0.0.0:7000
            data_dir = ./n3
            peer = 1 @ 10.0.0.1:7000
            peer = 2 @ 10.0.0.2:7000,10.0.0.2:7001
            gc_grace = 900s
            metrics = 127.0.0.1:9464
            admin_socket = n3.sock
        ";
        let c = NodeConfig::parse(text, Path::new("/etc/vals")).unwrap();
        assert_eq!(c.metrics, Some("127.0.0.1:9464".parse().unwrap()));
        assert_eq!(
            c.admin_socket.as_deref(),
            Some(Path::new("/etc/vals/n3.sock"))
        );
        assert_eq!(c.node_id, 3);
        assert_eq!(c.profile, Profile::Capacity);
        assert_eq!(c.backend, Backend::File);
        assert_eq!(c.data_dir.as_deref(), Some(Path::new("/etc/vals/n3")));
        assert_eq!(c.peers.len(), 2);
        assert_eq!(c.peers[1].1.len(), 2);
        assert_eq!(c.rf, 3);
        assert_eq!(c.sweep, Duration::from_secs(5));
        assert_eq!(c.gc_grace, Duration::from_secs(900));
        assert_eq!(c.block_size, 4096);
        assert_eq!(c.zone_region_mib, c.region_mib / 8);
        assert!(c.zone_region_mib_overrides.is_empty());
    }

    #[test]
    fn unknown_keys_typos_and_dupes_are_errors_with_line_numbers() {
        let bad = "node_id = 1\nlisten = 1.2.3.4:1\ndata_dir = d\nliste = oops";
        let e = NodeConfig::parse(bad, Path::new(".")).unwrap_err();
        assert_eq!(e.line, 4);
        assert!(e.msg.contains("unknown key `liste`"), "{e}");

        let dup = "node_id = 1\nnode_id = 2\nlisten = 1.2.3.4:1\ndata_dir = d";
        let e = NodeConfig::parse(dup, Path::new(".")).unwrap_err();
        assert_eq!(e.line, 2);
        assert!(e.msg.contains("more than once"), "{e}");

        let nodur = "node_id = 1\nlisten = 1.2.3.4:1\ndata_dir = d\nsweep = 5";
        let e = NodeConfig::parse(nodur, Path::new(".")).unwrap_err();
        assert!(e.msg.contains("suffix"), "{e}");
    }

    #[test]
    fn node_profiles_parse_validate_and_couple_features() {
        for (value, expected, blind, directory) in [
            ("capacity", Profile::Capacity, false, false),
            ("latency", Profile::Latency, true, true),
        ] {
            let text = format!(
                "node_id=1\nlisten=1.2.3.4:1\ndata_dir=d\nrf=1\nprofile={value}"
            );
            let c = NodeConfig::parse(&text, Path::new(".")).unwrap();
            assert_eq!(c.profile, expected);
            assert_eq!(c.profile.blind_append(), blind);
            assert_eq!(c.profile.winner_directory(), directory);
        }

        let invalid = "node_id=1\nlisten=1.2.3.4:1\ndata_dir=d\nprofile=fast";
        let e = NodeConfig::parse(invalid, Path::new(".")).unwrap_err();
        assert_eq!(e.line, 4);
        assert!(e.msg.contains("capacity|latency"), "{e}");

        let duplicate =
            "node_id=1\nlisten=1.2.3.4:1\ndata_dir=d\nprofile=capacity\nprofile=latency";
        let e = NodeConfig::parse(duplicate, Path::new(".")).unwrap_err();
        assert_eq!(e.line, 5);
        assert!(e.msg.contains("more than once"), "{e}");
    }

    #[test]
    fn backend_validation_catches_missing_pieces() {
        let e = NodeConfig::parse("node_id=1\nlisten=1.2.3.4:1", Path::new(".")).unwrap_err();
        assert!(e.msg.contains("data_dir"), "{e}");
        let e = NodeConfig::parse("node_id=1\nlisten=1.2.3.4:1\nbackend=spdk", Path::new("."))
            .unwrap_err();
        assert!(e.msg.contains("pci"), "{e}");
        let e = NodeConfig::parse(
            "node_id=1\nlisten=1.2.3.4:1\nlisten=1.2.3.4:2\ndata_dir=d",
            Path::new("."),
        )
        .unwrap_err();
        assert!(e.msg.contains("exactly one"), "{e}");

        let c = NodeConfig::parse(
            "node_id=1\nlisten=1.2.3.4:1\nbackend=spdk\npci=0000:a1:00.0\nrf=1\ngc_grace=900s",
            Path::new("."),
        )
        .unwrap();
        assert_eq!(c.backend, Backend::Spdk);
        assert_eq!(c.gc_grace, Duration::from_secs(900));
        assert!(c.data_dir.is_none(), "spdk needs no data_dir for the lease");

        let legacy_false = NodeConfig::parse(
            "node_id=1\nlisten=1.2.3.4:1\nbackend=spdk\npci=0000:a1:00.0\nrf=1\nformat=false",
            Path::new("."),
        );
        assert!(legacy_false.is_ok(), "format=false remains an inert migration aid");
        let legacy_true = NodeConfig::parse(
            "node_id=1\nlisten=1.2.3.4:1\nbackend=spdk\npci=0000:a1:00.0\nrf=1\nformat=true",
            Path::new("."),
        )
        .unwrap_err();
        assert!(legacy_true.msg.contains("valsd provision"), "{legacy_true}");

        let c = NodeConfig::parse(
            "node_id=1\nlisten=1.2.3.4:1\nbackend=spdk\npci=0000:a1:00.0\nrf=1\ngc_grace=900s\ndata_dir=d",
            Path::new("."),
        )
        .unwrap();
        assert_eq!(c.backend, Backend::Spdk);
        assert_eq!(c.gc_grace, Duration::from_secs(900));
    }

    #[test]
    fn jbod_zone_region_overrides_parse_and_validate() {
        let base = "node_id=1\nlisten=1.2.3.4:1\nlisten=1.2.3.4:2\n\
                    backend=spdk\npci=0000:a1:00.0\nrf=1\nregion_mib=1024\n";
        let c = NodeConfig::parse(
            &format!(
                "{base}zone_region_mib=128\n\
                 zone_region_mib_override=0:1\n\
                 zone_region_mib_override=7:256\n"
            ),
            Path::new("."),
        )
        .unwrap();
        assert_eq!(c.zone_region_mib, 128);
        assert_eq!(c.zone_region_mib_overrides, vec![(0, 1), (7, 256)]);

        for (suffix, needle) in [
            ("zone_region_mib_override=7", "expected `<zone-id>:<MiB>`"),
            (
                "zone_region_mib_override=7:1\nzone_region_mib_override=7:2",
                "duplicate zone id 7",
            ),
            ("zone_region_mib_override=7:0", "must be >= 1"),
            (
                "seg_blocks=512\nzone_region_mib=2\nzone_region_mib_override=7:1",
                "at least one segment",
            ),
            ("zone_region_mib_override=7:2048", "must fit"),
        ] {
            let e = NodeConfig::parse(&format!("{base}{suffix}\n"), Path::new(".")).unwrap_err();
            assert!(e.msg.contains(needle), "{suffix:?}: {e}");
        }

        let misaligned = format!(
            "{base}seg_blocks=192\nzone_region_mib=3\nzone_region_mib_override=7:1\n"
        );
        let e = NodeConfig::parse(&misaligned, Path::new(".")).unwrap_err();
        assert!(e.msg.contains("segment-aligned"), "{e}");

        let single = "node_id=1\nlisten=1.2.3.4:1\nbackend=spdk\n\
                      pci=0000:a1:00.0\nrf=1\nzone_region_mib_override=0:1";
        let e = NodeConfig::parse(single, Path::new(".")).unwrap_err();
        assert!(e.msg.contains("only on the sharded/JBOD"), "{e}");
    }

    #[test]
    fn data_node_weight_parses_and_validates() {
        let text = "
            pd_id = 1
            data_dir = pd
            data_node = 1 @ 10.0.0.1:7000
            data_node = 2*8 @ 10.0.0.2:7000
            rf = 2
        ";
        let c = PdConfig::parse(text, Path::new("/x")).unwrap();
        assert_eq!(c.vnodes, vals_cluster::ring::DEFAULT_VNODES);
        assert_eq!(c.state_capacity_mib, 256);
        assert_eq!(
            c.data_nodes[0],
            (1, 1, vec!["10.0.0.1:7000".parse().unwrap()])
        );
        assert_eq!(
            c.data_nodes[1],
            (2, 8, vec!["10.0.0.2:7000".parse().unwrap()])
        );

        let e = PdConfig::parse(
            "pd_id=1\ndata_dir=d\ndata_node = 2*0 @ 1.2.3.4:1",
            Path::new("."),
        )
        .unwrap_err();
        assert_eq!(e.line, 3);
        assert!(e.msg.contains("weight must be >= 1"), "{e}");

        let e = PdConfig::parse(
            "pd_id=1\ndata_dir=d\ndata_node = 2*x @ 1.2.3.4:1",
            Path::new("."),
        )
        .unwrap_err();
        assert_eq!(e.line, 3);
        assert!(e.msg.contains("not a number"), "{e}");
    }

    #[test]
    fn pd_join_needs_peers_and_excludes_campaign() {
        let base = "pd_id = 4\ndata_dir = pd4\nlisten = 127.0.0.1:7100\ndata_node = 1 @ 10.0.0.1:7000\nrf = 1\n";
        let peers = "pd_peer = 1 @ 10.0.0.1:7100\npd_peer = 4 @ 10.0.0.4:7100\n";
        let c = PdConfig::parse(&format!("{base}{peers}join = true\n"), Path::new("/x")).unwrap();
        assert!(c.join && !c.campaign);
        assert!(!PdConfig::parse(&format!("{base}{peers}"), Path::new("/x")).unwrap().join);
        for bad in [format!("{base}join = true\n"), format!("{base}{peers}join = true\ncampaign = true\n")] {
            let e = PdConfig::parse(&bad, Path::new("/x")).unwrap_err();
            assert!(e.msg.contains("`join = true`"), "{e}");
        }
    }

    #[test]
    fn pd_config_parses_zones_and_multi_pd() {
        let text = "
            pd_id = 2
            data_dir = pd2
            listen = 0.0.0.0:7100
            pd_peer = 1 @ 10.0.0.1:7100
            pd_peer = 3 @ 10.0.0.3:7100
            campaign = false
            data_node = 1 @ 10.0.0.1:7000
            data_node = 2 @ 10.0.0.2:7000,10.0.0.2:7001
            data_node = 3 @ 10.0.0.3:7000
            rf = 3
            vnodes = 32
            state_capacity_mib = 384
            zone = events 3 sorted
            zone = sessions 2 unsorted
        ";
        let c = PdConfig::parse(text, Path::new("/x")).unwrap();
        assert_eq!(c.pd_id, 2);
        assert_eq!(
            c.pd_peers,
            vec![
                (1, "10.0.0.1:7100".parse().unwrap()),
                (3, "10.0.0.3:7100".parse().unwrap())
            ]
        );
        assert_eq!(c.data_nodes[1].2.len(), 2);
        assert_eq!(c.vnodes, 32);
        assert_eq!(c.state_capacity_mib, 384);
        assert_eq!(c.zones.len(), 2);
        assert_eq!(c.zones[0].name, "events");
        assert_eq!(c.zones[0].ordering, Ordering::Sorted);
        assert_eq!(c.admin_socket, PathBuf::from("/x/pd2/admin.sock"));

        let e = PdConfig::parse("pd_id=1\ndata_dir=d\npd_peer=2@1.2.3.4:1", Path::new("."))
            .unwrap_err();
        assert!(e.msg.contains("requires `listen`"), "{e}");
        let e = PdConfig::parse("pd_id=1\ndata_dir=d\nstate_capacity_mib=0", Path::new("."))
            .unwrap_err();
        assert!(e.msg.contains("must be a nonzero"), "{e}");
    }

    #[test]
    fn node_geometry_and_cadence_reject_zero_and_overflow() {
        let base = "node_id=1\nlisten=1.2.3.4:1\ndata_dir=d\n";
        for (setting, needle) in [
            ("block_size=0", "block_size"),
            ("seg_blocks=0", "seg_blocks"),
            ("region_mib=0", "region_mib"),
            ("sweep=0s", "sweep"),
            ("sweep=18446744073709551615s", "too large"),
            ("region_mib=18446744073709551615", "too large"),
            (
                "block_size=1\nseg_blocks=1\nregion_mib=4097",
                "packed segment-index",
            ),
            (
                "block_size=4096\nseg_blocks=1048576\nregion_mib=4096",
                "packed segment-offset",
            ),
        ] {
            let error = NodeConfig::parse(&format!("{base}{setting}"), Path::new("."))
                .unwrap_err();
            assert!(error.msg.contains(needle), "{setting}: {error}");
        }
        let error = NodeConfig::parse(
            "node_id=1\nlisten=1.2.3.4:1\ndata_dir=d\nrf=2",
            Path::new("."),
        )
        .unwrap_err();
        assert!(error.msg.contains("exceeds boot membership"), "{error}");
        let error = NodeConfig::parse(
            "node_id=1\nlisten=1.2.3.4:1\ndata_dir=d\nrf=2\npeer=1@1.2.3.4:2",
            Path::new("."),
        )
        .unwrap_err();
        assert!(error.msg.contains("duplicate node id"), "{error}");
        #[cfg(target_pointer_width = "64")]
        {
            let error = NodeConfig::parse(
                "node_id=1\nlisten=1.2.3.4:1\ndata_dir=d\nrf=4294967296",
                Path::new("."),
            )
            .unwrap_err();
            assert!(error.msg.contains("wire-format"), "{error}");
        }
    }

    #[test]
    fn pd_config_rejects_duplicate_malformed_and_under_replicated_inputs() {
        let base = "pd_id=1\ndata_dir=d\ndata_node=1@1.2.3.4:1\nrf=1\n";
        for (suffix, needle) in [
            ("data_node=1@1.2.3.4:2", "duplicate node id"),
            ("zone=broken", "expected `<name>"),
            ("zone=z 0 sorted", "RF must be >= 1"),
            ("zone=z 2 sorted", "eligible membership"),
            ("zone=z 1 sorted\nzone=z 1 unsorted", "duplicate zone name"),
            ("vnodes=0", "vnodes"),
        ] {
            let error = PdConfig::parse(&format!("{base}{suffix}"), Path::new("."))
                .unwrap_err();
            assert!(error.msg.contains(needle), "{suffix}: {error}");
        }
        let error = PdConfig::parse(
            "pd_id=1\ndata_dir=d\ndata_node=1@1.2.3.4:1\nrf=2",
            Path::new("."),
        )
        .unwrap_err();
        assert!(error.msg.contains("exceeds configured data-node membership"), "{error}");
    }

    #[test]
    fn log_level_parses_and_defaults_and_errors() {
        let c =
            NodeConfig::parse("node_id=1\nlisten=1.2.3.4:1\ndata_dir=d\nrf=1", Path::new(".")).unwrap();
        assert_eq!(c.log_level, None);

        for (v, want) in [
            ("error", log::LevelFilter::Error),
            ("warn", log::LevelFilter::Warn),
            ("info", log::LevelFilter::Info),
            ("debug", log::LevelFilter::Debug),
            ("trace", log::LevelFilter::Trace),
            ("off", log::LevelFilter::Off),
        ] {
            let text = format!("node_id=1\nlisten=1.2.3.4:1\ndata_dir=d\nrf=1\nlog_level={v}");
            let c = NodeConfig::parse(&text, Path::new(".")).unwrap();
            assert_eq!(c.log_level, Some(want), "log_level={v}");
        }

        let c = PdConfig::parse(
            "pd_id=1\ndata_dir=d\ndata_node=1@1.2.3.4:1\nrf=1\nlog_level=debug",
            Path::new("."),
        ).unwrap();
        assert_eq!(c.log_level, Some(log::LevelFilter::Debug));

        let e = NodeConfig::parse(
            "node_id=1\nlisten=1.2.3.4:1\ndata_dir=d\nlog_level=loud",
            Path::new("."),
        )
        .unwrap_err();
        assert_eq!(e.line, 4);
        assert!(e.msg.contains("error|warn|info|debug|trace|off"), "{e}");
    }
}
