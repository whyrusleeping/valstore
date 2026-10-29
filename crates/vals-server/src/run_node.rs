use std::net::TcpListener;

use vals_cluster::catalog_store::{CatalogStore, FileCatalogStore};
use vals_cluster::liveness::{FileLivenessLog, MemLivenessLog};
use vals_cluster::node::NodeHandle;
use vals_cluster::zone::Catalog;
use vals_cluster::zonestore::{FileZoneStore, MemZoneStore, ZoneGeom, ZoneStore};
use vals_cluster::Ordering;
use vals_cluster::{Node, Ring, DEFAULT_ZONE};

use crate::config::{Backend, NodeConfig};

pub type Error = Box<dyn std::error::Error>;

pub fn resolve_settings(cfg: &NodeConfig) -> Result<(vals_cluster::node_settings::NodeSettings, Vec<String>), String> {
    let (settings, legacy) = cfg.settings.resolve(|name| std::env::var_os(name).map(|value| value.to_string_lossy().into_owned()))?;
    Ok((settings.resolve_ram_defaults(), legacy))
}

pub fn run_node(cfg: &NodeConfig, settings: vals_cluster::node_settings::NodeSettings) -> Result<NodeHandle, Error> {
    log::info!("{}", crate::version_line());
    settings.configure_process()?;
    log::info!("effective node settings: {settings:?}");
    log::info!("effective transport limits: {:?}", settings.transport_limits());
    log::info!("effective node cadence: flush={:?} compact={:?}; cleanup_min_confirms={}",
        settings.flush_interval(cfg.sweep), settings.compact_interval(cfg.sweep),
        settings.cleanup_min_confirms.unwrap_or(usize::MAX));
    vals_cluster::replica::configure_blind_append(cfg.profile.blind_append()).map_err(|active| {
        format!("deployment profile conflicts with process-stable blind-append state {active}")
    })?;
    vals_store::configure_winner_directory(cfg.profile.winner_directory()).map_err(|active| {
        format!("deployment profile conflicts with process-stable winner-directory state {active}")
    })?;
    crate::metrics::set_profile(cfg.profile == crate::config::Profile::Latency);
    log::info!(
        "node {}: deployment profile={} blind_append={} winner_directory={}",
        cfg.node_id,
        cfg.profile,
        cfg.profile.blind_append(),
        cfg.profile.winner_directory(),
    );

    let mut ring = Ring::new(cfg.rf);
    let mut members: Vec<(u32, u32)> = vec![(cfg.node_id, 1)];
    members.extend(cfg.peers.iter().map(|(id, _)| (*id, 1)));
    ring.set_members_weighted(members);

    let peers: Vec<(u32, std::net::SocketAddr)> = cfg
        .peers
        .iter()
        .map(|(id, addrs)| (*id, addrs[0]))
        .collect();

    let grace_ms = cfg.gc_grace.as_millis() as u64;

    if let Some(addr) = cfg.metrics {
        crate::metrics::spawn(addr).map_err(|e| format!("bind metrics {addr}: {e}"))?;
    }

    match cfg.backend {
        Backend::File => {
            let dir = cfg.data_dir.as_ref().expect("validated by config");
            std::fs::create_dir_all(dir)?;
            let geom = ZoneGeom {
                block_size: cfg.block_size,
                seg_blocks: cfg.seg_blocks,
                reserved_blocks: 0,
                region_blocks: cfg.region_mib * 1024 * 1024 / cfg.block_size as u64,
            };
            let provider = FileZoneStore::new(cfg.node_id, dir.join("zones"), geom)?;
            let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted)?;
            let cat_path = dir.join("catalog");
            let boot_catalog = FileCatalogStore::new(&cat_path).load().unwrap_or_default();
            let listener = bind(cfg.listen[0])?;
            log::info!(
                "node {}: serving file-backed zones from {} on {} ({} boot-catalog zones)",
                cfg.node_id,
                dir.display(),
                cfg.listen[0],
                boot_catalog.len(),
            );
            Ok(if grace_ms > 0 {
                Node::serve_zoned_with_liveness_and_settings(
                    cfg.node_id,
                    listener,
                    zone0,
                    provider,
                    boot_catalog,
                    ring,
                    peers,
                    cfg.sweep,
                    grace_ms,
                    FileLivenessLog::new(dir.join("liveness")),
                    Some(Box::new(FileCatalogStore::new(&cat_path))),
                    settings,
                )
            } else {
                Node::serve_zoned_with_settings(
                    cfg.node_id,
                    listener,
                    zone0,
                    provider,
                    boot_catalog,
                    ring,
                    peers,
                    cfg.sweep,
                    Some(Box::new(FileCatalogStore::new(&cat_path))),
                    settings,
                )
            })
        }
        Backend::Mem => {
            let geom = ZoneGeom {
                block_size: cfg.block_size,
                seg_blocks: cfg.seg_blocks,
                reserved_blocks: 0,
                region_blocks: cfg.region_mib * 1024 * 1024 / cfg.block_size as u64,
            };
            let provider = MemZoneStore::new(cfg.node_id, geom);
            let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted)?;
            let listener = bind(cfg.listen[0])?;
            log::info!(
                "node {}: serving in-memory zones on {} (data is lost on exit)",
                cfg.node_id,
                cfg.listen[0]
            );
            Ok(if grace_ms > 0 {
                Node::serve_zoned_with_liveness_and_settings(
                    cfg.node_id,
                    listener,
                    zone0,
                    provider,
                    Catalog::new(),
                    ring,
                    peers,
                    cfg.sweep,
                    grace_ms,
                    MemLivenessLog::new(),
                    None,
                    settings,
                )
            } else {
                Node::serve_zoned_with_settings(
                    cfg.node_id,
                    listener,
                    zone0,
                    provider,
                    Catalog::new(),
                    ring,
                    peers,
                    cfg.sweep,
                    None,
                    settings,
                )
            })
        }
        Backend::Spdk => serve_spdk(cfg, ring, settings),
    }
}

fn bind(addr: std::net::SocketAddr) -> Result<TcpListener, Error> {
    let listener = TcpListener::bind(addr).map_err(|e| format!("bind {addr}: {e}"))?;
    listener.set_nonblocking(true)?;
    Ok(listener)
}

#[cfg(feature = "spdk")]
fn serve_spdk(cfg: &NodeConfig, ring: Ring, settings: vals_cluster::node_settings::NodeSettings) -> Result<NodeHandle, Error> {
    use vals_cluster::drive::{PartitionGeom, SpdkDriveSet};

    vals_spdk::spdk::spdk_env_init(&vals_spdk::spdk::SpdkEnv {
        name: Some(format!("vals{}", cfg.node_id)),
        shm_id: cfg.spdk_shm_id,
        core: cfg.spdk_core,
        mem_mb: cfg.spdk_mem_mb,
    })
    .map_err(|e| format!("spdk env init: {e:?}"))?;

    if cfg.pci.len() == 1 && cfg.listen.len() == 1 {
        return serve_spdk_zoned(cfg, ring, settings);
    }

    let grace_ms = cfg.gc_grace.as_millis() as u64;
    let driveset = SpdkDriveSet::attach(&cfg.pci).map_err(|e| format!("spdk attach: {e:?}"))?;
    let listeners = cfg
        .listen
        .iter()
        .map(|&a| bind(a))
        .collect::<Result<Vec<_>, _>>()?;
    let geom = PartitionGeom {
        seg_blocks: cfg.seg_blocks,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    };
    let region_blocks = cfg.region_mib * 1024 * 1024 / 4096;
    let zone_region_blocks = cfg.zone_region_mib * 1024 * 1024 / 4096;
    let zone_region_overrides = cfg
        .zone_region_mib_overrides
        .iter()
        .map(|&(zone_id, mib)| (zone_id, mib * 1024 * 1024 / 4096))
        .collect::<Vec<_>>();
    let zone_super_blocks = 16u64;
    let peer_shards = cfg.peers.clone();
    let (weight, handle) = if grace_ms > 0 {
        Node::serve_multidrive_zoned_with_liveness_overrides_and_settings(
            cfg.node_id,
            listeners,
            driveset,
            region_blocks,
            zone_super_blocks,
            zone_region_blocks,
            zone_region_overrides,
            geom,
            ring,
            peer_shards,
            cfg.sweep,
            false,
            grace_ms,
            |_, provider| provider.liveness_log(),
            settings,
        )
        .map_err(|e| format!("multidrive layout: {e:?}"))?
    } else {
        Node::serve_multidrive_zoned_with_overrides_and_settings(
            cfg.node_id,
            listeners,
            driveset,
            region_blocks,
            zone_super_blocks,
            zone_region_blocks,
            zone_region_overrides,
            geom,
            ring,
            peer_shards,
            cfg.sweep,
            false,
            settings,
        )
        .map_err(|e| format!("multidrive layout: {e:?}"))?
    };
    log::info!(
        "node {}: serving {} SPDK partitions over {} drives, full catalog per \
         partition ({} MiB fallback/zone, overrides {:?}; sorted zones sharded \
         intra-node by range token). Ring weight {weight} — advertise it: \
         `valsd admin add-node {} {weight} <addrs>`",
        cfg.node_id,
        cfg.listen.len(),
        cfg.pci.len(),
        cfg.zone_region_mib,
        cfg.zone_region_mib_overrides,
        cfg.node_id,
    );
    Ok(handle)
}

#[cfg(feature = "spdk")]
fn serve_spdk_zoned(cfg: &NodeConfig, ring: Ring, settings: vals_cluster::node_settings::NodeSettings) -> Result<NodeHandle, Error> {
    use vals_cluster::zone::Catalog;
    use vals_cluster::zonestore::{SpdkZoneConfig, SpdkZoneStore};
    use vals_spdk::spdk::{SpdkConfig, SpdkController};

    let controller = SpdkController::attach(&SpdkConfig {
        pci_addr: cfg.pci[0].clone(),
        nsid: 1,
    })
    .map_err(|e| format!("spdk attach {}: {e}", cfg.pci[0]))?;

    let region_blocks = cfg.region_mib * 1024 * 1024 / 4096;
    let provider = SpdkZoneStore::open(
        cfg.node_id,
        controller,
        SpdkZoneConfig {
            super_blocks: 16,
            region_blocks,
            seg_blocks: cfg.seg_blocks,
            reserved_blocks: 0,
        },
        false,
    )
    .map_err(|e| format!("spdk zone store: {e}"))?;
    let zone0 = provider
        .open_existing(DEFAULT_ZONE)
        .map_err(|e| format!("open default zone: {e}"))?;

    let listener = bind(cfg.listen[0])?;
    let peers: Vec<(u32, std::net::SocketAddr)> = cfg
        .peers
        .iter()
        .map(|(id, addrs)| (*id, addrs[0]))
        .collect();
    log::info!(
        "node {}: serving SPDK multi-zone (sorted+unsorted) over {} on {} \
         ({} MiB/zone region)",
        cfg.node_id,
        cfg.pci[0],
        cfg.listen[0],
        cfg.region_mib,
    );
    let grace_ms = cfg.gc_grace.as_millis() as u64;
    Ok(if grace_ms > 0 {
        let live = provider.liveness_log();
        Node::serve_zoned_with_liveness_and_settings(
            cfg.node_id,
            listener,
            zone0,
            provider,
            Catalog::new(),
            ring,
            peers,
            cfg.sweep,
            grace_ms,
            live,
            None,
            settings,
        )
    } else {
        Node::serve_zoned_with_settings(
            cfg.node_id,
            listener,
            zone0,
            provider,
            Catalog::new(),
            ring,
            peers,
            cfg.sweep,
            None,
            settings,
        )
    })
}

#[cfg(not(feature = "spdk"))]
fn serve_spdk(cfg: &NodeConfig, _ring: Ring, _settings: vals_cluster::node_settings::NodeSettings) -> Result<NodeHandle, Error> {
    let _ = (
        &cfg.pci,
        cfg.spdk_shm_id,
        cfg.spdk_mem_mb,
        cfg.spdk_core,
        cfg.zone_region_mib,
        &cfg.zone_region_mib_overrides,
    );
    Err(
        "backend = spdk requires a `--features spdk` build (VALS_SPDK_LINK=1)"
            .to_string()
            .into(),
    )
}
