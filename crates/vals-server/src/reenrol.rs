use std::path::PathBuf;
use std::process::ExitCode;

use crate::config::{Backend, NodeConfig};

type Error = Box<dyn std::error::Error>;

pub const USAGE: &str =
    "valsd reenrol -c <config> (--incarnation <32-hex-digit id> | --clear)";

#[derive(Debug)]
struct Args {
    config: PathBuf,
    incarnation: Option<u128>,
}

fn parse_incarnation(text: &str) -> Result<u128, String> {
    if text.len() != 32 || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(format!(
            "--incarnation must be the 32 hex digits shown by the PD's `status` (got {text:?})"
        ));
    }
    match u128::from_str_radix(text, 16) {
        Ok(0) | Err(_) => Err("--incarnation must be a non-zero 32-hex-digit id".into()),
        Ok(id) => Ok(id),
    }
}

fn parse_args(args: &[String]) -> Result<Args, String> {
    let mut config = None;
    let mut target: Option<Option<u128>> = None;
    let mut i = 0;
    while i < args.len() {
        let flag = args[i].as_str();
        match flag {
            "--clear" if target.is_none() => {
                target = Some(None);
                i += 1;
                continue;
            }
            "-c" | "--incarnation" => {}
            "--clear" => return Err("give exactly one of --incarnation or --clear".into()),
            _ => return Err(format!("unknown reenrol flag {flag:?}")),
        }
        let value = args
            .get(i + 1)
            .ok_or_else(|| format!("{flag} requires a value"))?;
        match flag {
            "-c" if config.is_none() => config = Some(PathBuf::from(value)),
            "--incarnation" if target.is_none() => target = Some(Some(parse_incarnation(value)?)),
            "-c" => return Err("duplicate flag -c".into()),
            _ => return Err("give exactly one of --incarnation or --clear".into()),
        }
        i += 2;
    }
    Ok(Args {
        config: config.ok_or_else(|| "reenrol requires `-c <node-config>`".to_string())?,
        incarnation: target
            .ok_or_else(|| "reenrol requires `--incarnation <id>` or `--clear`".to_string())?,
    })
}

fn show(id: Option<u128>) -> String {
    id.map_or_else(|| "none".to_string(), |id| format!("{id:032x}"))
}

pub fn reenrol_main(raw_args: &[String]) -> ExitCode {
    let args = match parse_args(raw_args) {
        Ok(args) => args,
        Err(e) => {
            eprintln!("valsd reenrol: {e}\nusage: {USAGE}");
            return ExitCode::from(2);
        }
    };
    let cfg = match NodeConfig::load(&args.config) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("valsd reenrol: {}: {e}", args.config.display());
            return ExitCode::from(2);
        }
    };
    vals_cluster::observe::init_stderr_logger();
    super::apply_log_level(cfg.log_level);
    match reenrol(&cfg, args.incarnation) {
        Ok(previous) => {
            for (shard, before) in previous.iter().enumerate() {
                println!(
                    "node {} shard {shard}: cluster incarnation {} -> {}",
                    cfg.node_id,
                    show(*before),
                    show(args.incarnation)
                );
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("valsd reenrol: {e}");
            ExitCode::FAILURE
        }
    }
}

fn reenrol(cfg: &NodeConfig, incarnation: Option<u128>) -> Result<Vec<Option<u128>>, Error> {
    match cfg.backend {
        Backend::File => {
            use vals_cluster::zonestore::{FileZoneStore, ZoneGeom, ZoneStore};
            let dir = cfg.data_dir.as_ref().expect("validated by config").join("zones");
            if !dir.is_dir() {
                return Err(format!("{} holds no node data", dir.display()).into());
            }
            let store = FileZoneStore::new(
                cfg.node_id,
                dir,
                ZoneGeom {
                    block_size: cfg.block_size,
                    seg_blocks: cfg.seg_blocks,
                    reserved_blocks: 0,
                    region_blocks: cfg.region_mib * 1024 * 1024 / cfg.block_size as u64,
                },
            )?;
            let before = store.persisted_incarnation();
            store.persist_incarnation(incarnation)?;
            Ok(vec![before])
        }
        Backend::Mem => Err("backend = mem keeps nothing across restarts; nothing to re-enrol".into()),
        Backend::Spdk => reenrol_spdk(cfg, incarnation),
    }
}

#[cfg(feature = "spdk")]
fn reenrol_spdk(cfg: &NodeConfig, incarnation: Option<u128>) -> Result<Vec<Option<u128>>, Error> {
    use vals_cluster::drive::{PartitionGeom, SpdkDriveSet};
    use vals_cluster::zonestore::{SpdkZoneConfig, SpdkZoneStore, ZoneStore};
    use vals_cluster::{Node, Ordering};
    use vals_spdk::spdk::{spdk_env_init, SpdkConfig, SpdkController, SpdkEnv};

    let _device_locks = crate::spdk_lock::DeviceLocks::acquire(&cfg.pci)?;
    spdk_env_init(&SpdkEnv {
        name: Some(format!("vals{}-reenrol", cfg.node_id)),
        shm_id: -1,
        core: cfg.spdk_core,
        mem_mb: cfg.spdk_mem_mb,
    })
    .map_err(|e| format!("spdk env init: {e}"))?;
    let mib_blocks = 1024u64 * 1024 / 4096;
    let region_blocks = cfg.region_mib * mib_blocks;
    if cfg.pci.len() == 1 && cfg.listen.len() == 1 {
        let controller = SpdkController::attach(&SpdkConfig {
            pci_addr: cfg.pci[0].clone(),
            nsid: 1,
        })
        .map_err(|e| format!("spdk attach {}: {e}", cfg.pci[0]))?;
        let store = SpdkZoneStore::open(
            cfg.node_id,
            controller,
            SpdkZoneConfig {
                super_blocks: 16,
                region_blocks,
                seg_blocks: cfg.seg_blocks,
                reserved_blocks: 0,
            },
            false,
        )?;
        let before = store.persisted_incarnation();
        store.persist_incarnation(incarnation)?;
        return Ok(vec![before]);
    }
    let driveset = SpdkDriveSet::attach(&cfg.pci).map_err(|e| format!("spdk attach: {e:?}"))?;
    let overrides = cfg
        .zone_region_mib_overrides
        .iter()
        .map(|&(zone_id, mib)| (zone_id, mib * mib_blocks))
        .collect();
    Node::reenrol_multidrive_zoned(
        cfg.node_id,
        &driveset,
        cfg.listen.len(),
        region_blocks,
        16,
        cfg.zone_region_mib * mib_blocks,
        overrides,
        PartitionGeom {
            seg_blocks: cfg.seg_blocks,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
        incarnation,
    )
    .map_err(|e| {
        format!(
            "multidrive layout: {e:?}; shards already rewritten keep the new id, so rerun the \
             same command"
        )
        .into()
    })
}

#[cfg(not(feature = "spdk"))]
fn reenrol_spdk(_cfg: &NodeConfig, _incarnation: Option<u128>) -> Result<Vec<Option<u128>>, Error> {
    Err("backend = spdk requires a `--features spdk` build (VALS_SPDK_LINK=1)".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn args(list: &[&str]) -> Result<Args, String> {
        parse_args(&list.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }

    #[test]
    fn reenrol_requires_one_exact_target() {
        let id = "0123456789abcdef0123456789ABCDEF";
        let parsed = args(&["-c", "n.conf", "--incarnation", id]).unwrap();
        assert_eq!(parsed.incarnation, Some(0x0123456789abcdef0123456789abcdefu128));
        assert_eq!(args(&["--clear", "-c", "n.conf"]).unwrap().incarnation, None);
        assert!(args(&["-c", "n.conf"]).is_err());
        assert!(args(&["-c", "n.conf", "--clear", "--incarnation", id]).is_err());
        assert!(args(&["-c", "n.conf", "--incarnation", "abc"]).is_err());
        assert!(args(&["-c", "n.conf", "--incarnation", &"0".repeat(32)]).is_err());
        assert!(args(&["-c", "n.conf", "--incarnation", &format!("+{}", &id[1..])]).is_err());
        assert!(args(&["--incarnation", id]).is_err());
    }

    #[test]
    fn file_backend_reenrol_rewrites_and_clears_the_persisted_id() {
        use vals_cluster::zonestore::{FileZoneStore, ZoneGeom, ZoneStore};
        let dir = std::env::temp_dir().join(format!("vals-reenrol-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let cfg = NodeConfig::parse(
            &format!("node_id=3\nlisten=127.0.0.1:1\ndata_dir={}\nrf=1", dir.display()),
            Path::new("."),
        )
        .unwrap();
        assert!(reenrol(&cfg, Some(7)).is_err(), "no node data yet");
        std::fs::create_dir_all(dir.join("zones")).unwrap();
        assert_eq!(reenrol(&cfg, Some(7)).unwrap(), vec![None]);
        assert_eq!(reenrol(&cfg, Some(9)).unwrap(), vec![Some(7)]);
        let geom = ZoneGeom { block_size: 512, seg_blocks: 8, reserved_blocks: 0, region_blocks: 64 };
        let store = FileZoneStore::new(3, dir.join("zones"), geom).unwrap();
        assert_eq!(store.persisted_incarnation(), Some(9));
        assert_eq!(reenrol(&cfg, None).unwrap(), vec![Some(9)]);
        let store = FileZoneStore::new(3, dir.join("zones"), geom).unwrap();
        assert_eq!(store.persisted_incarnation(), None);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
