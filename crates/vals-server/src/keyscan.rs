use std::path::PathBuf;
use std::process::ExitCode;

use crate::config::{Backend, NodeConfig};

type Error = Box<dyn std::error::Error>;
pub const USAGE: &str = "usage: valsd keyscan -c <config> --zone-id <id> --out-dir <dir> [--want-ids <file>] [--partition <N>]";

struct Args {
    config: PathBuf,
    zone_id: u32,
    out: PathBuf,
    want: Option<PathBuf>,
    partition: Option<usize>,
}

fn parse(args: &[String]) -> Result<Args, Error> {
    let mut config = None;
    let mut zone_id = None;
    let mut out = None;
    let mut want = None;
    let mut partition = None;
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let value = it.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "-c" | "--config" => config = Some(PathBuf::from(value)),
            "--zone-id" => zone_id = Some(value.parse()?),
            "--out-dir" => out = Some(PathBuf::from(value)),
            "--want-ids" => want = Some(PathBuf::from(value)),
            "--partition" => partition = Some(value.parse()?),
            _ => return Err(format!("unknown flag {flag}").into()),
        }
    }
    Ok(Args {
        config: config.ok_or("missing -c")?, zone_id: zone_id.ok_or("missing --zone-id")?,
        out: out.ok_or("missing --out-dir")?, want, partition,
    })
}

pub fn keyscan_main(args: &[String]) -> ExitCode {
    let args = match parse(args) {
        Ok(a) => a,
        Err(e) => { eprintln!("valsd keyscan: {e}\n{USAGE}"); return ExitCode::from(2); }
    };
    let result = (|| -> Result<(), Error> {
        let cfg = NodeConfig::load(&args.config)?;
        if cfg.backend != Backend::Spdk {
            return Err("keyscan supports the SPDK multidrive-zoned layout only".into());
        }
        run(&cfg, &args)
    })();
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => { eprintln!("valsd keyscan: {e}"); ExitCode::FAILURE }
    }
}

#[cfg(feature = "spdk")]
fn run(cfg: &NodeConfig, args: &Args) -> Result<(), Error> {
    use vals_cluster::drive::SpdkDriveSet;
    use vals_cluster::keyscan::{keyscan, read_want_ids, KeyscanConfig};
    use vals_cluster::zonestore::RegionZoneConfig;
    use vals_spdk::spdk::{spdk_env_init, SpdkEnv};

    if cfg.pci.len() == 1 && cfg.listen.len() == 1 {
        return Err("keyscan requires multidrive-zoned geometry (not the single-listener SpdkZoneStore layout)".into());
    }
    if args.partition.is_some_and(|p| p >= cfg.listen.len()) {
        return Err("partition out of range".into());
    }
    let want = args.want.as_deref().map(read_want_ids).transpose()?;
    let _locks = crate::spdk_lock::DeviceLocks::acquire(&cfg.pci)?;
    spdk_env_init(&SpdkEnv {
        name: Some(format!("valskeyscan{}", cfg.node_id)), shm_id: -1,
        core: cfg.spdk_core, mem_mb: cfg.spdk_mem_mb,
    }).map_err(|e| format!("spdk env init: {e:?}"))?;
    let ds = SpdkDriveSet::attach(&cfg.pci).map_err(|e| format!("spdk attach: {e:?}"))?;
    keyscan(&ds, &KeyscanConfig {
        node_id: cfg.node_id, partitions: cfg.listen.len(), region_blocks: cfg.region_mib * 256,
        zones: RegionZoneConfig {
            super_blocks: 16, zone_region_blocks: cfg.zone_region_mib * 256,
            zone_region_overrides: cfg.zone_region_mib_overrides.iter().map(|&(id, mib)| (id, mib * 256)).collect(),
            seg_blocks: cfg.seg_blocks, reserved_blocks: 0,
        },
        zone_id: args.zone_id, partition: args.partition,
    }, &args.out, want.as_ref())
}

#[cfg(not(feature = "spdk"))]
fn run(_cfg: &NodeConfig, args: &Args) -> Result<(), Error> {
    let _ = (&args.zone_id, &args.out, &args.want, &args.partition);
    Err("backend = spdk requires a `--features spdk` build (VALS_SPDK_LINK=1)".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn keyscan_args() {
        let args = ["-c", "node.conf", "--zone-id", "2", "--out-dir", "out", "--partition", "3", "--want-ids", "want.txt"].map(String::from);
        let a = parse(&args).unwrap();
        assert_eq!(a.zone_id, 2);
        assert_eq!(a.partition, Some(3));
        assert_eq!(a.want, Some(PathBuf::from("want.txt")));
        assert!(parse(&args[..4]).is_err());
        assert!(parse(&["--zone-id".into(), "no".into()]).is_err());
    }
}
