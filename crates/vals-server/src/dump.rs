use std::path::{Path, PathBuf};

use vals_cluster::catalog_store::{CatalogStore, FileCatalogStore};
use vals_cluster::zone::ZoneId;
use vals_store::{Config, Ordering};

use crate::config::{Backend, NodeConfig};

type Error = Box<dyn std::error::Error>;

struct DumpArgs {
    config: PathBuf,
    zone: ZoneSel,
    out: PathBuf,
    skip_tombstones: bool,
    parallel: usize,
}

enum ZoneSel {
    Name(String),
    Id(ZoneId),
}

fn parse_args(args: &[String]) -> Result<DumpArgs, String> {
    let mut config: Option<PathBuf> = None;
    let mut name: Option<String> = None;
    let mut id: Option<ZoneId> = None;
    let mut out: Option<PathBuf> = None;
    let mut skip_tombstones = false;
    let mut parallel: usize = 1;

    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "-c" | "--config" => config = Some(PathBuf::from(next(&mut it, a)?)),
            "--zone" => name = Some(next(&mut it, a)?),
            "--zone-id" => {
                let v = next(&mut it, a)?;
                id = Some(
                    v.parse()
                        .map_err(|_| format!("--zone-id: not a number: {v:?}"))?,
                );
            }
            "--out" | "-o" => out = Some(PathBuf::from(next(&mut it, a)?)),
            "--skip-tombstones" => skip_tombstones = true,
            "--parallel" | "-j" => {
                let v = next(&mut it, a)?;
                let n: usize = v
                    .parse()
                    .map_err(|_| format!("--parallel: not a number: {v:?}"))?;
                if n < 1 {
                    return Err("--parallel must be >= 1".into());
                }
                parallel = n.min(64);
            }
            other => return Err(format!("unknown flag {other:?}")),
        }
    }

    let config = config.ok_or("missing required -c <config>")?;
    let out = out.ok_or("missing required --out <path>")?;
    let zone = match (name, id) {
        (Some(_), Some(_)) => return Err("give --zone or --zone-id, not both".into()),
        (Some(n), None) => ZoneSel::Name(n),
        (None, Some(i)) => ZoneSel::Id(i),
        (None, None) => return Err("missing --zone <name> or --zone-id <id>".into()),
    };
    Ok(DumpArgs {
        config,
        zone,
        out,
        skip_tombstones,
        parallel,
    })
}

fn next(it: &mut std::slice::Iter<'_, String>, flag: &str) -> Result<String, String> {
    it.next()
        .cloned()
        .ok_or_else(|| format!("{flag} needs a value"))
}

pub fn dump_main(args: &[String]) -> std::process::ExitCode {
    let args = match parse_args(args) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("valsd dump: {e}\n{}", USAGE);
            return std::process::ExitCode::from(2);
        }
    };
    match run_dump(&args) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("valsd dump: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

pub const USAGE: &str = "usage: valsd dump -c <config> (--zone <name> | --zone-id <id>) \
     --out <path> [--skip-tombstones] [--parallel <N>]";

fn run_dump(args: &DumpArgs) -> Result<(), Error> {
    let cfg =
        NodeConfig::load(&args.config).map_err(|e| format!("{}: {e}", args.config.display()))?;
    match cfg.backend {
        Backend::File => dump_file(&cfg, args),
        Backend::Mem => {
            Err("backend = mem keeps zones only in RAM — nothing on disk to dump".into())
        }
        Backend::Spdk => dump_spdk(&cfg, args),
    }
}

fn dump_file(cfg: &NodeConfig, args: &DumpArgs) -> Result<(), Error> {
    use vals_spdk::FileBlockDevice;

    let data_dir = cfg
        .data_dir
        .as_ref()
        .ok_or("backend = file requires `data_dir` in the config")?;
    let zones_dir = data_dir.join("zones");

    let (zone_id, zone_name) = match &args.zone {
        ZoneSel::Id(id) => (*id, format!("zone-{id}")),
        ZoneSel::Name(name) => {
            let catalog = FileCatalogStore::new(data_dir.join("catalog"))
                .load()
                .ok_or_else(|| {
                    format!(
                        "no persisted catalog at {} to resolve --zone {name:?}; \
                         pass --zone-id <id> instead",
                        data_dir.join("catalog").display()
                    )
                })?;
            let id = catalog
                .by_name(name)
                .ok_or_else(|| format!("zone {name:?} not in the persisted catalog"))?;
            (id, name.clone())
        }
    };

    let vlog_path = zones_dir.join(format!("zone-{zone_id}.vlog"));
    if !vlog_path.exists() {
        return Err(missing_zone_err(&zones_dir, zone_id).into());
    }

    let dev = FileBlockDevice::open(&vlog_path, cfg.block_size)
        .map_err(|e| format!("open {}: {e}", vlog_path.display()))?;
    let scan_cfg = Config {
        seg_blocks: cfg.seg_blocks,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    };
    run_emit(&dev, &scan_cfg, &zone_name, args)
}

fn missing_zone_err(zones_dir: &Path, zone_id: ZoneId) -> String {
    let mut ids: Vec<ZoneId> = Vec::new();
    if let Ok(rd) = std::fs::read_dir(zones_dir) {
        for ent in rd.flatten() {
            if let Some(name) = ent.file_name().to_str() {
                if let Some(id) = name
                    .strip_prefix("zone-")
                    .and_then(|s| s.strip_suffix(".vlog"))
                    .and_then(|s| s.parse::<ZoneId>().ok())
                {
                    ids.push(id);
                }
            }
        }
    }
    ids.sort_unstable();
    format!(
        "zone {zone_id} has no vlog at {}; existing zone ids: {:?}",
        zones_dir.join(format!("zone-{zone_id}.vlog")).display(),
        ids
    )
}

#[cfg(feature = "spdk")]
fn dump_spdk(cfg: &NodeConfig, args: &DumpArgs) -> Result<(), Error> {
    use vals_cluster::zonestore::{verify_topology, RegionZoneConfig, ZoneTopology};
    use vals_cluster::{RegionTable, SuperblockStore};
    use vals_spdk::spdk::{spdk_env_init, SpdkConfig, SpdkController, SpdkEnv};
    use vals_spdk::BlockDevice;

    let zone_id = match &args.zone {
        ZoneSel::Id(id) => *id,
        ZoneSel::Name(_) => {
            return Err("backend = spdk does not persist zone names on-device; \
                        use --zone-id <id>"
                .into())
        }
    };
    if cfg.pci.len() != 1 || cfg.listen.len() != 1 {
        return Err("backend = spdk dump requires a single-drive, single-listen topology; \
                    use keyscan for multi-partition layouts. If changing topology, \
                    run `valsd provision` while the node is offline".into());
    }

    let _device_locks = crate::spdk_lock::DeviceLocks::acquire(&cfg.pci)?;
    spdk_env_init(&SpdkEnv {
        name: Some(format!("valsdump{}", cfg.node_id)),
        shm_id: -1,
        core: cfg.spdk_core,
        mem_mb: cfg.spdk_mem_mb,
    })
    .map_err(|e| format!("spdk env init: {e:?}"))?;

    let controller = SpdkController::attach(&SpdkConfig {
        pci_addr: cfg.pci[0].clone(),
        nsid: 1,
    })
    .map_err(|e| format!("spdk attach {}: {e}", cfg.pci[0]))?;

    let super_blocks: u64 = 16;
    let region_blocks = cfg.region_mib * 1024 * 1024 / 4096;
    let total_blocks = controller.num_blocks();
    let super_dev = controller
        .open_shard(0, super_blocks)
        .map_err(|e| format!("open superblock shard: {e}"))?;
    let block_size = super_dev.block_size();
    let super_store = SuperblockStore::new(super_dev, super_blocks / 2);
    let (_, regions, _, topology, _) = super_store
        .load()
        .map_err(|e| format!("load superblock: {e}"))?
        .ok_or("no superblock on the drive; run `valsd provision` while the node is offline")?;
    let expected = ZoneTopology::single(cfg.node_id, total_blocks, &RegionZoneConfig {
        super_blocks, zone_region_blocks: region_blocks, zone_region_overrides: Vec::new(),
        seg_blocks: cfg.seg_blocks, reserved_blocks: 0,
    }, block_size);
    verify_topology(topology, expected)?;
    let table = RegionTable::from_regions(super_blocks, region_blocks, total_blocks, regions)
        .map_err(|e| format!("region table: {e}"))?;
    let region = table
        .get(zone_id)
        .ok_or_else(|| format!("zone id {zone_id} has no region on this drive"))?;

    let dev = controller
        .open_shard(region.base, region.len)
        .map_err(|e| format!("open zone shard: {e}"))?;
    let scan_cfg = Config {
        seg_blocks: cfg.seg_blocks,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    };
    run_emit(&dev, &scan_cfg, &format!("zone-{zone_id}"), args)
}

#[cfg(not(feature = "spdk"))]
fn dump_spdk(_cfg: &NodeConfig, _args: &DumpArgs) -> Result<(), Error> {
    Err("backend = spdk requires a `--features spdk` build (VALS_SPDK_LINK=1)".into())
}

fn run_emit<D: vals_spdk::BlockDevice>(
    dev: &D,
    scan_cfg: &Config,
    zone_name: &str,
    args: &DumpArgs,
) -> Result<(), Error> {
    let stats = vals_cluster::dump::emit_dump(
        dev,
        scan_cfg,
        zone_name,
        &args.out,
        args.skip_tombstones,
        args.parallel,
        None,
    )?;
    eprintln!();
    eprintln!(
        "valsd dump: {}",
        stats.summary(
            zone_name,
            &args.out,
            args.skip_tombstones,
            args.parallel,
            None,
        )
    );
    Ok(())
}
