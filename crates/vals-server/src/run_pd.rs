use std::sync::mpsc;
use std::time::{Duration, Instant};

use vals_cluster::pd::{PdDaemon, RingConfig};
use vals_cluster::zone::ZoneConfig;

use crate::admin::{self, AdminReq};
use crate::config::PdConfig;

pub type Error = Box<dyn std::error::Error>;

pub fn resolve_placement(
    cfg: &PdConfig,
) -> Result<(vals_cluster::pd::PdPlacementSettings, Vec<&'static str>), String> {
    cfg.placement.resolve(|name| {
        std::env::var_os(name).map(|value| value.to_string_lossy().into_owned())
    })
}

pub fn run_pd(cfg: &PdConfig) -> Result<(PdDaemon, mpsc::Receiver<AdminReq>), Error> {
    let (placement, legacy) = resolve_placement(cfg)?;
    log::info!("effective PD placement settings: {placement:?}");
    if !legacy.is_empty() {
        log::warn!(
            "deprecated PD environment settings: {}; migrate to PD config keys (docs/configuration.md)",
            legacy.join(", "),
        );
    }
    let transport_limits = vals_cluster::transport::TransportLimits::from_env()?;
    vals_cluster::transport::configure(transport_limits)?;
    log::info!("effective transport limits: {transport_limits:?}");
    std::fs::create_dir_all(&cfg.data_dir)?;
    let raft_dir = cfg.data_dir.join("raft");

    if let Some(addr) = cfg.metrics {
        crate::metrics::spawn(addr).map_err(|e| format!("bind metrics {addr}: {e}"))?;
    }

    let state_capacity = cfg.state_capacity_mib * 1024 * 1024;
    log::info!(
        "pd {}: bounded materialized state capacity {} MiB",
        cfg.pd_id,
        cfg.state_capacity_mib
    );
    let ring = RingConfig {
        vnodes: cfg.vnodes,
        rf: cfg.rf,
    };

    let push_nodes: Vec<(vals_cluster::NodeId, Vec<std::net::SocketAddr>)> = cfg
        .data_nodes
        .iter()
        .map(|(id, _w, addrs)| (*id, addrs.clone()))
        .collect();

    let pd = if cfg.pd_peers.is_empty() {
        log::info!(
            "pd {}: single-replica PD, raft state in {}",
            cfg.pd_id,
            raft_dir.display()
        );
        PdDaemon::spawn_single_durable_bounded_with_settings(
            cfg.pd_id,
            state_capacity,
            ring,
            push_nodes.clone(),
            &raft_dir,
            placement,
        )?
    } else {
        let listen = cfg.listen.expect("validated by config");
        let listener = std::net::TcpListener::bind(listen)
            .map_err(|e| format!("bind raft port {listen}: {e}"))?;
        log::info!(
            "pd {}: raft group of {} (peer port {listen}), state in {}",
            cfg.pd_id,
            cfg.pd_peers.iter().map(|(id, _)| *id).chain([cfg.pd_id]).collect::<std::collections::BTreeSet<_>>().len(),
            raft_dir.display()
        );
        if cfg.join {
            PdDaemon::spawn_joining_bounded_with_settings(
                cfg.pd_id,
                listener,
                cfg.pd_peers.clone(),
                push_nodes.clone(),
                state_capacity,
                ring,
                &raft_dir,
                placement,
            )?
        } else {
            PdDaemon::spawn_durable_bounded_with_settings(
                cfg.pd_id,
                listener,
                cfg.pd_peers.clone(),
                push_nodes.clone(),
                state_capacity,
                ring,
                cfg.campaign,
                &raft_dir,
                placement,
            )?
        }
    };

    if let Some(digest) = crate::backup::read_restore_seed(&cfg.data_dir)? {
        log::info!(
            "pd {}: restored by restore-pd with state digest {digest:08x}; confirming it to the PD group",
            cfg.pd_id
        );
        pd.confirm_restore_seed(digest);
    }
    seed_boot_state(&pd, cfg);
    let admin_rx = admin::spawn_listener(&cfg.admin_socket)?;
    log::info!(
        "pd {}: admin socket {}",
        cfg.pd_id,
        cfg.admin_socket.display()
    );
    Ok((pd, admin_rx))
}

fn seed_boot_state(pd: &PdDaemon, cfg: &PdConfig) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while pd.members().is_empty() && pd.catalog().ids().is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }

    let members = pd.members();
    if members.is_empty() {
        log::info!(
            "pd: atomically installing {} boot members",
            cfg.data_nodes.len()
        );
        pd.bootstrap_members(cfg.data_nodes.clone());
    } else {
        for (id, weight, addrs) in &cfg.data_nodes {
            if members.contains(id) {
                continue;
            }
            log::info!(
                "pd: adding boot member {id} (weight {weight}, {} shard addrs)",
                addrs.len()
            );
            pd.add_boot_node_addressed(*id, *weight, addrs.clone());
        }
    }

    let catalog = pd.catalog();
    for z in &cfg.zones {
        if catalog.by_name(&z.name).is_some() {
            log::info!("pd: boot zone {:?} already committed", z.name);
            continue;
        }
        log::info!("pd: creating boot zone {:?} (rf {})", z.name, z.rf);
        pd.create_boot_zone(ZoneConfig {
            name: z.name.clone(),
            rf: z.rf,
            ordering: z.ordering,
            scope: None,
        });
    }
}
