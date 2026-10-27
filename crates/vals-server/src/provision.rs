use std::collections::HashMap;
use std::path::PathBuf;
use std::process::ExitCode;

use crate::config::{Backend, NodeConfig};

type Error = Box<dyn std::error::Error>;

struct Args {
    config: PathBuf,
    devices: Vec<(String, String)>,
    confirmation: String,
}

fn parse_args(args: &[String]) -> Result<Args, String> {
    let mut config = None;
    let mut devices = Vec::new();
    let mut confirmation = None;
    let mut i = 0;
    while i < args.len() {
        let flag = &args[i];
        let value = args
            .get(i + 1)
            .ok_or_else(|| format!("{flag} requires a value"))?;
        match flag.as_str() {
            "-c" if config.is_none() => config = Some(PathBuf::from(value)),
            "--device" => {
                let (pci, serial) = value.split_once('=').ok_or_else(|| {
                    "--device must be an exact `<pci-address>=<serial-number>` pair".to_string()
                })?;
                if pci.is_empty() || serial.is_empty() {
                    return Err(
                        "--device must be an exact `<pci-address>=<serial-number>` pair".into(),
                    );
                }
                devices.push((pci.to_string(), serial.to_string()));
            }
            "--confirm-destroy" if confirmation.is_none() => {
                confirmation = Some(value.to_string())
            }
            "-c" | "--confirm-destroy" => return Err(format!("duplicate flag {flag}")),
            _ => return Err(format!("unknown provision flag {flag:?}")),
        }
        i += 2;
    }
    Ok(Args {
        config: config.ok_or_else(|| "provision requires `-c <node-config>`".to_string())?,
        devices,
        confirmation: confirmation
            .ok_or_else(|| "provision requires `--confirm-destroy node-<id>`".to_string())?,
    })
}

fn validate_request(cfg: &NodeConfig, args: &Args) -> Result<HashMap<String, String>, String> {
    if cfg.backend != Backend::Spdk {
        return Err("provision requires `backend = spdk`".into());
    }
    let expected_confirmation = format!("node-{}", cfg.node_id);
    if args.confirmation != expected_confirmation {
        return Err(format!(
            "destructive confirmation mismatch: expected `--confirm-destroy {expected_confirmation}`"
        ));
    }
    let mut expected = HashMap::new();
    for (pci, serial) in &args.devices {
        if expected.insert(pci.clone(), serial.clone()).is_some() {
            return Err(format!("duplicate --device identity {pci:?}"));
        }
    }
    if cfg.pci.iter().any(|pci| !expected.contains_key(pci))
        || expected.keys().any(|pci| !cfg.pci.contains(pci))
        || expected.len() != cfg.pci.len()
    {
        return Err(format!(
            "--device identities must exactly match configured `pci` entries {:?}",
            cfg.pci
        ));
    }
    Ok(expected)
}

pub fn provision_main(raw_args: &[String]) -> ExitCode {
    let args = match parse_args(raw_args) {
        Ok(args) => args,
        Err(e) => {
            eprintln!("valsd provision: {e}");
            return ExitCode::from(2);
        }
    };
    let cfg = match NodeConfig::load(&args.config) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("valsd provision: {}: {e}", args.config.display());
            return ExitCode::from(2);
        }
    };
    let devices = match validate_request(&cfg, &args) {
        Ok(devices) => devices,
        Err(e) => {
            eprintln!("valsd provision: {e}");
            return ExitCode::from(2);
        }
    };
    vals_cluster::observe::init_stderr_logger();
    super::apply_log_level(cfg.log_level);
    match provision_spdk(&cfg, &devices) {
        Ok(()) => {
            println!(
                "provisioned node {} on {} device(s); ordinary `valsd node` starts now open \
                 this layout",
                cfg.node_id,
                cfg.pci.len()
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("valsd provision: {e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(feature = "spdk")]
fn provision_spdk(cfg: &NodeConfig, expected: &HashMap<String, String>) -> Result<(), Error> {
    use vals_cluster::drive::{PartitionGeom, SpdkDriveSet};
    use vals_cluster::zonestore::{SpdkZoneConfig, SpdkZoneStore};
    use vals_cluster::{Node, Ordering};
    use vals_spdk::spdk::{spdk_env_init, SpdkConfig, SpdkController, SpdkEnv};
    use vals_spdk::BlockDevice;

    let (settings, legacy) = crate::run_node::resolve_settings(cfg)?;
    for alias in legacy { log::warn!("legacy environment setting {alias}; migrate to node config"); }
    let _device_locks = crate::spdk_lock::DeviceLocks::acquire(&cfg.pci)?;
    spdk_env_init(&SpdkEnv {
        name: Some(format!("vals{}-provision", cfg.node_id)),
        shm_id: -1,
        core: cfg.spdk_core,
        mem_mb: cfg.spdk_mem_mb,
    })
    .map_err(|e| format!("spdk env init: {e}"))?;

    let mut controllers = Vec::with_capacity(cfg.pci.len());
    for pci in &cfg.pci {
        let controller = SpdkController::attach(&SpdkConfig {
            pci_addr: pci.clone(),
            nsid: 1,
        })
        .map_err(|e| format!("exclusive SPDK attach {pci}: {e}"))?;
        let actual = controller.serial_number()?;
        let wanted = &expected[pci];
        if actual != *wanted {
            return Err(format!(
                "device identity mismatch for {pci}: expected serial {wanted:?}, attached serial \
                 {actual:?}; no layout was written"
            )
            .into());
        }
        controllers.push(controller);
    }

    if settings.device_dealloc_segments_per_tick > 0 {
        for (pci, controller) in cfg.pci.iter().zip(&controllers) {
            log::info!("provision: deallocating whole namespace on {pci} ({} blocks)", controller.num_blocks());
            let dev = controller.open_shard(0, controller.num_blocks())?;
            match dev.deallocate(&[(0, dev.num_blocks())]) {
                Ok(true) => log::info!("provision: namespace deallocated on {pci}"),
                Ok(false) => log::warn!("provision: deallocation unsupported on {pci}; continuing with header wipe"),
                Err(e) => return Err(format!("namespace deallocation failed on {pci}; provisioning aborted: {e}").into()),
            }
        }
    } else {
        log::info!("provision: device deallocation disabled by device_dealloc_segments_per_tick=0");
    }

    let mib_blocks = 1024u64 * 1024 / 4096;
    let region_blocks = cfg
        .region_mib
        .checked_mul(mib_blocks)
        .ok_or("region_mib overflows SPDK block geometry")?;
    if cfg.pci.len() == 1 && cfg.listen.len() == 1 {
        SpdkZoneStore::provision(
            cfg.node_id,
            controllers.pop().unwrap(),
            SpdkZoneConfig {
                super_blocks: 16,
                region_blocks,
                seg_blocks: cfg.seg_blocks,
                reserved_blocks: 0,
            },
        )?;
        return Ok(());
    }

    let zone_region_blocks = cfg
        .zone_region_mib
        .checked_mul(mib_blocks)
        .ok_or("zone_region_mib overflows SPDK block geometry")?;
    let zone_region_overrides = cfg
        .zone_region_mib_overrides
        .iter()
        .map(|&(zone_id, mib)| {
            mib.checked_mul(mib_blocks)
                .map(|blocks| (zone_id, blocks))
                .ok_or("zone_region_mib_override overflows SPDK block geometry")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let driveset = SpdkDriveSet::from_controllers(controllers);
    Node::provision_multidrive_zoned(
        cfg.node_id,
        &driveset,
        cfg.listen.len(),
        region_blocks,
        16,
        zone_region_blocks,
        zone_region_overrides,
        PartitionGeom {
            seg_blocks: cfg.seg_blocks,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    )?;
    Ok(())
}

#[cfg(not(feature = "spdk"))]
fn provision_spdk(_cfg: &NodeConfig, _expected: &HashMap<String, String>) -> Result<(), Error> {
    Err("provision requires a `--features spdk` build (VALS_SPDK_LINK=1)".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn cfg() -> NodeConfig {
        NodeConfig::parse(
            "node_id=7\nlisten=127.0.0.1:7000\nbackend=spdk\npci=0000:01:00.0\npci=0000:02:00.0\nrf=1",
            Path::new("."),
        )
        .unwrap()
    }

    #[test]
    fn destructive_request_requires_exact_devices_serials_and_confirmation() {
        let args = parse_args(&[
            "-c".into(),
            "node.conf".into(),
            "--device".into(),
            "0000:01:00.0=SERIAL-A".into(),
            "--device".into(),
            "0000:02:00.0=SERIAL-B".into(),
            "--confirm-destroy".into(),
            "node-7".into(),
        ])
        .unwrap();
        let validated = validate_request(&cfg(), &args).unwrap();
        assert_eq!(validated["0000:01:00.0"], "SERIAL-A");

        let mut wrong_confirmation = args;
        wrong_confirmation.confirmation = "node-8".into();
        assert!(validate_request(&cfg(), &wrong_confirmation).is_err());

        wrong_confirmation.confirmation = "node-7".into();
        wrong_confirmation.devices.pop();
        assert!(validate_request(&cfg(), &wrong_confirmation).is_err());
    }

    #[test]
    fn malformed_device_identity_is_rejected_before_provisioning() {
        let err = parse_args(&[
            "-c".into(),
            "node.conf".into(),
            "--device".into(),
            "0000:01:00.0".into(),
            "--confirm-destroy".into(),
            "node-7".into(),
        ])
        .err()
        .unwrap();
        assert!(err.contains("<pci-address>=<serial-number>"), "{err}");
    }
}
