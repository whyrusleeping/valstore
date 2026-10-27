use super::*;

impl Node {
    pub fn bind() -> io::Result<(TcpListener, SocketAddr)> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        Ok((listener, addr))
    }

    pub fn bind_n(count: usize) -> io::Result<Vec<(TcpListener, SocketAddr)>> {
        (0..count).map(|_| Self::bind()).collect()
    }

    pub fn serve<D>(
        node_id: NodeId,
        listener: TcpListener,
        store: VersionedStore<D>,
        ring: Ring,
        peers: Vec<(NodeId, SocketAddr)>,
        sweep_every: Duration,
    ) -> NodeHandle
    where
        D: BlockDevice + AsyncRead + Send + 'static,
    {
        let (mut maint, maint_join) = crate::maint::spawn_pool(1);
        NodeHandle {
            shards: vec![spawn_shard(
                listener,
                peers,
                sweep_every,
                maint.pop().unwrap(),
                move || ServerShared::single_zone_on(node_id, store, ring),
            )],
            maint_join,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn serve_with_liveness<D, L>(
        node_id: NodeId,
        listener: TcpListener,
        store: VersionedStore<D>,
        ring: Ring,
        peers: Vec<(NodeId, SocketAddr)>,
        sweep_every: Duration,
        gc_grace_ms: u64,
        liveness_log: L,
    ) -> NodeHandle
    where
        D: BlockDevice + AsyncRead + Send + 'static,
        L: LivenessLog + Send + 'static,
    {
        let (mut maint, maint_join) = crate::maint::spawn_pool(1);
        NodeHandle {
            shards: vec![spawn_shard(
                listener,
                peers,
                sweep_every,
                maint.pop().unwrap(),
                move || {
                    ServerShared::single_zone_on(node_id, store, ring)
                        .with_liveness(gc_grace_ms, Box::new(liveness_log))
                },
            )],
            maint_join,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn serve_zoned<D, Z>(
        node_id: NodeId, listener: TcpListener, zone0_store: VersionedStore<D>, provider: Z,
        catalog: crate::zone::Catalog, ring: Ring, peers: Vec<(NodeId, SocketAddr)>,
        sweep_every: Duration, catalog_store: Option<Box<dyn crate::catalog_store::CatalogStore + Send>>,
    ) -> NodeHandle
    where D: BlockDevice + AsyncRead + Send + 'static, Z: crate::zonestore::ZoneStore<D> + Send + 'static,
    {
        Self::serve_zoned_with_settings(node_id, listener, zone0_store, provider, catalog, ring,
            peers, sweep_every, catalog_store, crate::node_settings::NodeSettings::default())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn serve_zoned_with_settings<D, Z>(
        node_id: NodeId,
        listener: TcpListener,
        zone0_store: VersionedStore<D>,
        provider: Z,
        catalog: crate::zone::Catalog,
        ring: Ring,
        peers: Vec<(NodeId, SocketAddr)>,
        sweep_every: Duration,
        catalog_store: Option<Box<dyn crate::catalog_store::CatalogStore + Send>>,
        settings: crate::node_settings::NodeSettings,
    ) -> NodeHandle
    where
        D: BlockDevice + AsyncRead + Send + 'static,
        Z: crate::zonestore::ZoneStore<D> + Send + 'static,
    {
        let (mut maint, maint_join) = crate::maint::spawn_pool_lanes(1, settings.maint_compact_lanes);
        NodeHandle {
            shards: vec![spawn_shard(
                listener,
                peers,
                sweep_every,
                maint.pop().unwrap(),
                move || {
                    let shared =
                        ServerShared::zoned(node_id, zone0_store, Rc::new(provider), catalog, ring)
                            .with_settings(settings);
                    match catalog_store {
                        Some(cs) => shared.with_catalog_store(cs),
                        None => shared,
                    }
                },
            )],
            maint_join,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn serve_zoned_with_liveness<D, Z, L>(
        node_id: NodeId, listener: TcpListener, zone0_store: VersionedStore<D>, provider: Z,
        catalog: crate::zone::Catalog, ring: Ring, peers: Vec<(NodeId, SocketAddr)>,
        sweep_every: Duration, gc_grace_ms: u64, liveness_log: L,
        catalog_store: Option<Box<dyn crate::catalog_store::CatalogStore + Send>>,
    ) -> NodeHandle
    where D: BlockDevice + AsyncRead + Send + 'static, Z: crate::zonestore::ZoneStore<D> + Send + 'static,
        L: LivenessLog + Send + 'static,
    {
        Self::serve_zoned_with_liveness_and_settings(node_id, listener, zone0_store, provider, catalog,
            ring, peers, sweep_every, gc_grace_ms, liveness_log, catalog_store,
            crate::node_settings::NodeSettings::default())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn serve_zoned_with_liveness_and_settings<D, Z, L>(
        node_id: NodeId,
        listener: TcpListener,
        zone0_store: VersionedStore<D>,
        provider: Z,
        catalog: crate::zone::Catalog,
        ring: Ring,
        peers: Vec<(NodeId, SocketAddr)>,
        sweep_every: Duration,
        gc_grace_ms: u64,
        liveness_log: L,
        catalog_store: Option<Box<dyn crate::catalog_store::CatalogStore + Send>>,
        settings: crate::node_settings::NodeSettings,
    ) -> NodeHandle
    where
        D: BlockDevice + AsyncRead + Send + 'static,
        Z: crate::zonestore::ZoneStore<D> + Send + 'static,
        L: LivenessLog + Send + 'static,
    {
        let (mut maint, maint_join) = crate::maint::spawn_pool_lanes(1, settings.maint_compact_lanes);
        NodeHandle {
            shards: vec![spawn_shard(
                listener,
                peers,
                sweep_every,
                maint.pop().unwrap(),
                move || {
                    let shared =
                        ServerShared::zoned(node_id, zone0_store, Rc::new(provider), catalog, ring)
                            .with_settings(settings)
                            .with_liveness(gc_grace_ms, Box::new(liveness_log));
                    match catalog_store {
                        Some(cs) => shared.with_catalog_store(cs),
                        None => shared,
                    }
                },
            )],
            maint_join,
        }
    }

    pub fn serve_sharded<D>(
        node_id: NodeId, shards: Vec<(TcpListener, VersionedStore<D>)>, ring: Ring,
        peer_shards: Vec<(NodeId, Vec<SocketAddr>)>, sweep_every: Duration,
    ) -> NodeHandle
    where D: BlockDevice + AsyncRead + Send + 'static,
    {
        Self::serve_sharded_with_settings(node_id, shards, ring, peer_shards, sweep_every,
            crate::node_settings::NodeSettings::default())
    }

    pub fn serve_sharded_with_settings<D>(
        node_id: NodeId,
        shards: Vec<(TcpListener, VersionedStore<D>)>,
        ring: Ring,
        peer_shards: Vec<(NodeId, Vec<SocketAddr>)>,
        sweep_every: Duration,
        settings: crate::node_settings::NodeSettings,
    ) -> NodeHandle
    where
        D: BlockDevice + AsyncRead + Send + 'static,
    {
        let shard_count = shards.len();
        let (maint, maint_join) = crate::maint::spawn_pool_lanes(shard_count, settings.maint_compact_lanes);
        let mut maint = maint.into_iter();
        let mut handle = NodeHandle {
            shards: Vec::new(),
            maint_join,
        };
        let own_shard_addrs: Vec<SocketAddr> = shards
            .iter()
            .filter_map(|(l, _)| l.local_addr().ok())
            .collect();
        for (s, (listener, store)) in shards.into_iter().enumerate() {
            let peers_s: Vec<(NodeId, SocketAddr)> = peer_shards
                .iter()
                .filter_map(|(p, addrs)| addrs.get(s).map(|&a| (*p, a)))
                .collect();
            let ring_s = ring.clone();
            let peer_shards_s = peer_shards.clone();
            let own_addrs_s = own_shard_addrs.clone();
            let settings = settings.clone();
            handle.shards.push(spawn_shard_at(
                listener,
                peers_s,
                s,
                sweep_every,
                maint.next().unwrap(),
                peer_shards_s,
                own_addrs_s,
                move || {
                    ServerShared::single_zone_on(node_id, store, ring_s).with_shard(s, shard_count)
                        .with_settings(settings)
                },
            ));
        }
        handle
    }

    #[allow(clippy::too_many_arguments)]
    pub fn serve_multidrive<DS>(
        node_id: NodeId, shards: Vec<TcpListener>, driveset: DS, region_blocks: u64,
        geom: crate::drive::PartitionGeom, ring: Ring, peer_shards: Vec<(NodeId, Vec<SocketAddr>)>,
        sweep_every: Duration, create: bool,
    ) -> Result<(u32, NodeHandle), crate::drive::LayoutError>
    where DS: crate::drive::DriveSet, DS::Dev: BlockDevice + AsyncRead + Send + 'static,
    {
        Self::serve_multidrive_with_settings(node_id, shards, driveset, region_blocks, geom, ring,
            peer_shards, sweep_every, create, crate::node_settings::NodeSettings::default())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn serve_multidrive_with_settings<DS>(
        node_id: NodeId,
        shards: Vec<TcpListener>,
        driveset: DS,
        region_blocks: u64,
        geom: crate::drive::PartitionGeom,
        ring: Ring,
        peer_shards: Vec<(NodeId, Vec<SocketAddr>)>,
        sweep_every: Duration,
        create: bool,
        settings: crate::node_settings::NodeSettings,
    ) -> Result<(u32, NodeHandle), crate::drive::LayoutError>
    where
        DS: crate::drive::DriveSet,
        DS::Dev: BlockDevice + AsyncRead + Send + 'static,
    {
        let partitions = shards.len();
        let caps = crate::drive::drive_caps(&driveset);
        let layout = crate::drive::plan(&caps, region_blocks, partitions, partitions)?;
        crate::drive::verify_layout(&driveset, &layout, create)?;
        let stores = crate::drive::build_stores(&driveset, &layout, geom, node_id, create)?;
        let weight = crate::drive::node_weight(driveset.drive_count());
        let shard_stores: Vec<(TcpListener, VersionedStore<DS::Dev>)> =
            shards.into_iter().zip(stores).collect();
        let handle = Self::serve_sharded_with_settings(node_id, shard_stores, ring, peer_shards, sweep_every, settings);
        Ok((weight, handle))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn serve_sharded_with_liveness<D, L, F>(
        node_id: NodeId,
        shards: Vec<(TcpListener, VersionedStore<D>)>,
        ring: Ring,
        peer_shards: Vec<(NodeId, Vec<SocketAddr>)>,
        sweep_every: Duration,
        gc_grace_ms: u64,
        mk_log: F,
    ) -> NodeHandle
    where
        D: BlockDevice + AsyncRead + Send + 'static,
        L: LivenessLog + Send + 'static,
        F: Fn(usize) -> L,
    {
        let shard_count = shards.len();
        let (maint, maint_join) = crate::maint::spawn_pool(shard_count);
        let mut maint = maint.into_iter();
        let mut handle = NodeHandle {
            shards: Vec::new(),
            maint_join,
        };
        let own_shard_addrs: Vec<SocketAddr> = shards
            .iter()
            .filter_map(|(l, _)| l.local_addr().ok())
            .collect();
        for (s, (listener, store)) in shards.into_iter().enumerate() {
            let peers_s: Vec<(NodeId, SocketAddr)> = peer_shards
                .iter()
                .filter_map(|(p, addrs)| addrs.get(s).map(|&a| (*p, a)))
                .collect();
            let ring_s = ring.clone();
            let log = mk_log(s);
            let peer_shards_s = peer_shards.clone();
            let own_addrs_s = own_shard_addrs.clone();
            handle.shards.push(spawn_shard_at(
                listener,
                peers_s,
                s,
                sweep_every,
                maint.next().unwrap(),
                peer_shards_s,
                own_addrs_s,
                move || {
                    ServerShared::single_zone_on(node_id, store, ring_s)
                        .with_shard(s, shard_count)
                        .with_liveness(gc_grace_ms, Box::new(log))
                },
            ));
        }
        handle
    }

    #[allow(clippy::too_many_arguments)]
    pub fn serve_multidrive_with_liveness<DS, L, F>(
        node_id: NodeId,
        shards: Vec<TcpListener>,
        driveset: DS,
        region_blocks: u64,
        geom: crate::drive::PartitionGeom,
        ring: Ring,
        peer_shards: Vec<(NodeId, Vec<SocketAddr>)>,
        sweep_every: Duration,
        create: bool,
        gc_grace_ms: u64,
        mk_log: F,
    ) -> Result<(u32, NodeHandle), crate::drive::LayoutError>
    where
        DS: crate::drive::DriveSet,
        DS::Dev: BlockDevice + AsyncRead + Send + 'static,
        L: LivenessLog + Send + 'static,
        F: Fn(usize) -> L,
    {
        let partitions = shards.len();
        let caps = crate::drive::drive_caps(&driveset);
        let layout = crate::drive::plan(&caps, region_blocks, partitions, partitions)?;
        crate::drive::verify_layout(&driveset, &layout, create)?;
        let stores = crate::drive::build_stores(&driveset, &layout, geom, node_id, create)?;
        let weight = crate::drive::node_weight(driveset.drive_count());
        let shard_stores: Vec<(TcpListener, VersionedStore<DS::Dev>)> =
            shards.into_iter().zip(stores).collect();
        let handle = Self::serve_sharded_with_liveness(
            node_id,
            shard_stores,
            ring,
            peer_shards,
            sweep_every,
            gc_grace_ms,
            mk_log,
        );
        Ok((weight, handle))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn provision_multidrive_zoned<DS>(
        node_id: NodeId,
        driveset: &DS,
        num_partitions: usize,
        region_blocks: u64,
        zone_super_blocks: u64,
        zone_region_blocks: u64,
        zone_region_overrides: Vec<(crate::zone::ZoneId, u64)>,
        geom: crate::drive::PartitionGeom,
    ) -> Result<u32, crate::drive::LayoutError>
    where
        DS: crate::drive::DriveSet,
        DS::Dev: BlockDevice + AsyncRead + Send + 'static,
    {
        let (weight, stores) = build_multidrive_zoned(
            node_id,
            driveset,
            num_partitions,
            region_blocks,
            zone_super_blocks,
            zone_region_blocks,
            zone_region_overrides,
            geom,
            true,
        )?;
        drop(stores);
        Ok(weight)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn reenrol_multidrive_zoned<DS>(
        node_id: NodeId,
        driveset: &DS,
        num_partitions: usize,
        region_blocks: u64,
        zone_super_blocks: u64,
        zone_region_blocks: u64,
        zone_region_overrides: Vec<(crate::zone::ZoneId, u64)>,
        geom: crate::drive::PartitionGeom,
        incarnation: Option<u128>,
    ) -> Result<Vec<Option<u128>>, crate::drive::LayoutError>
    where
        DS: crate::drive::DriveSet,
        DS::Dev: BlockDevice + AsyncRead + Send + 'static,
    {
        use crate::zonestore::{open_partition_zone_stores, RegionZoneConfig, ZoneStore};
        let caps = crate::drive::drive_caps(driveset);
        let layout = crate::drive::plan(&caps, region_blocks, num_partitions, num_partitions)?;
        let zcfg = RegionZoneConfig {
            super_blocks: zone_super_blocks,
            zone_region_blocks,
            zone_region_overrides,
            seg_blocks: geom.seg_blocks,
            reserved_blocks: geom.reserved_blocks,
        };
        let providers = open_partition_zone_stores(driveset, &layout, &zcfg, node_id, false)?;
        let mut previous = Vec::with_capacity(providers.len());
        for provider in &providers {
            previous.push(provider.persisted_incarnation());
            provider.persist_incarnation(incarnation).map_err(|e| {
                crate::drive::LayoutError::Io(io::Error::other(e.to_string()))
            })?;
        }
        Ok(previous)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn serve_multidrive_zoned<DS>(
        node_id: NodeId,
        shards: Vec<TcpListener>,
        driveset: DS,
        region_blocks: u64,
        zone_super_blocks: u64,
        zone_region_blocks: u64,
        geom: crate::drive::PartitionGeom,
        ring: Ring,
        peer_shards: Vec<(NodeId, Vec<SocketAddr>)>,
        sweep_every: Duration,
        create: bool,
    ) -> Result<(u32, NodeHandle), crate::drive::LayoutError>
    where
        DS: crate::drive::DriveSet,
        DS::Dev: BlockDevice + AsyncRead + Send + 'static,
    {
        Self::serve_multidrive_zoned_with_overrides(
            node_id,
            shards,
            driveset,
            region_blocks,
            zone_super_blocks,
            zone_region_blocks,
            Vec::new(),
            geom,
            ring,
            peer_shards,
            sweep_every,
            create,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn serve_multidrive_zoned_with_overrides<DS>(
        node_id: NodeId, shards: Vec<TcpListener>, driveset: DS, region_blocks: u64,
        zone_super_blocks: u64, zone_region_blocks: u64,
        zone_region_overrides: Vec<(crate::zone::ZoneId, u64)>, geom: crate::drive::PartitionGeom,
        ring: Ring, peer_shards: Vec<(NodeId, Vec<SocketAddr>)>, sweep_every: Duration, create: bool,
    ) -> Result<(u32, NodeHandle), crate::drive::LayoutError>
    where DS: crate::drive::DriveSet, DS::Dev: BlockDevice + AsyncRead + Send + 'static,
    {
        Self::serve_multidrive_zoned_with_overrides_and_settings(node_id, shards, driveset, region_blocks,
            zone_super_blocks, zone_region_blocks, zone_region_overrides, geom, ring, peer_shards,
            sweep_every, create, crate::node_settings::NodeSettings::default())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn serve_multidrive_zoned_with_overrides_and_settings<DS>(
        node_id: NodeId,
        shards: Vec<TcpListener>,
        driveset: DS,
        region_blocks: u64,
        zone_super_blocks: u64,
        zone_region_blocks: u64,
        zone_region_overrides: Vec<(crate::zone::ZoneId, u64)>,
        geom: crate::drive::PartitionGeom,
        ring: Ring,
        peer_shards: Vec<(NodeId, Vec<SocketAddr>)>,
        sweep_every: Duration,
        create: bool,
        settings: crate::node_settings::NodeSettings,
    ) -> Result<(u32, NodeHandle), crate::drive::LayoutError>
    where
        DS: crate::drive::DriveSet,
        DS::Dev: BlockDevice + AsyncRead + Send + 'static,
    {
        let (weight, built) = build_multidrive_zoned(
            node_id,
            &driveset,
            shards.len(),
            region_blocks,
            zone_super_blocks,
            zone_region_blocks,
            zone_region_overrides,
            geom,
            create,
        )?;
        let shard_count = shards.len();
        let (maint, maint_join) = crate::maint::spawn_pool_lanes(shard_count, settings.maint_compact_lanes);
        let mut maint = maint.into_iter();
        let mut handle = NodeHandle {
            shards: Vec::new(),
            maint_join,
        };
        let own_shard_addrs: Vec<SocketAddr> =
            shards.iter().filter_map(|l| l.local_addr().ok()).collect();
        if own_shard_addrs.len() != shard_count {
            log::warn!(
                "node {node_id}: resolved only {} of {shard_count} shard addresses; \
                 sorted-zone sibling-shard handoff may be incomplete this run",
                own_shard_addrs.len()
            );
        }
        for (s, (listener, (provider, zone0))) in shards.into_iter().zip(built).enumerate() {
            let settings = settings.clone();
            let peers_s: Vec<(NodeId, SocketAddr)> = peer_shards
                .iter()
                .filter_map(|(p, addrs)| addrs.get(s).map(|&a| (*p, a)))
                .collect();
            let ring_s = ring.clone();
            let peer_shards_s = peer_shards.clone();
            let own_addrs_s = own_shard_addrs.clone();
            handle.shards.push(spawn_shard_at(
                listener,
                peers_s,
                s,
                sweep_every,
                maint.next().unwrap(),
                peer_shards_s,
                own_addrs_s,
                move || {
                    ServerShared::zoned(
                        node_id,
                        zone0,
                        Rc::new(provider),
                        crate::zone::Catalog::new(),
                        ring_s,
                    )
                    .with_shard(s, shard_count)
                    .with_settings(settings)
                },
            ));
        }
        Ok((weight, handle))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn serve_multidrive_zoned_with_liveness<DS, L, F>(
        node_id: NodeId,
        shards: Vec<TcpListener>,
        driveset: DS,
        region_blocks: u64,
        zone_super_blocks: u64,
        zone_region_blocks: u64,
        geom: crate::drive::PartitionGeom,
        ring: Ring,
        peer_shards: Vec<(NodeId, Vec<SocketAddr>)>,
        sweep_every: Duration,
        create: bool,
        gc_grace_ms: u64,
        mk_log: F,
    ) -> Result<(u32, NodeHandle), crate::drive::LayoutError>
    where
        DS: crate::drive::DriveSet,
        DS::Dev: BlockDevice + AsyncRead + Send + 'static,
        L: LivenessLog + Send + 'static,
        F: Fn(usize, &crate::zonestore::RegionZoneStore<DS::Dev>) -> L,
    {
        Self::serve_multidrive_zoned_with_liveness_and_overrides(
            node_id,
            shards,
            driveset,
            region_blocks,
            zone_super_blocks,
            zone_region_blocks,
            Vec::new(),
            geom,
            ring,
            peer_shards,
            sweep_every,
            create,
            gc_grace_ms,
            mk_log,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn serve_multidrive_zoned_with_liveness_and_overrides<DS, L, F>(
        node_id: NodeId, shards: Vec<TcpListener>, driveset: DS, region_blocks: u64,
        zone_super_blocks: u64, zone_region_blocks: u64,
        zone_region_overrides: Vec<(crate::zone::ZoneId, u64)>, geom: crate::drive::PartitionGeom,
        ring: Ring, peer_shards: Vec<(NodeId, Vec<SocketAddr>)>, sweep_every: Duration, create: bool,
        gc_grace_ms: u64, mk_log: F,
    ) -> Result<(u32, NodeHandle), crate::drive::LayoutError>
    where DS: crate::drive::DriveSet, DS::Dev: BlockDevice + AsyncRead + Send + 'static,
        L: LivenessLog + Send + 'static,
        F: Fn(usize, &crate::zonestore::RegionZoneStore<DS::Dev>) -> L,
    {
        Self::serve_multidrive_zoned_with_liveness_overrides_and_settings(node_id, shards, driveset,
            region_blocks, zone_super_blocks, zone_region_blocks, zone_region_overrides, geom, ring,
            peer_shards, sweep_every, create, gc_grace_ms, mk_log, crate::node_settings::NodeSettings::default())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn serve_multidrive_zoned_with_liveness_overrides_and_settings<DS, L, F>(
        node_id: NodeId,
        shards: Vec<TcpListener>,
        driveset: DS,
        region_blocks: u64,
        zone_super_blocks: u64,
        zone_region_blocks: u64,
        zone_region_overrides: Vec<(crate::zone::ZoneId, u64)>,
        geom: crate::drive::PartitionGeom,
        ring: Ring,
        peer_shards: Vec<(NodeId, Vec<SocketAddr>)>,
        sweep_every: Duration,
        create: bool,
        gc_grace_ms: u64,
        mk_log: F,
        settings: crate::node_settings::NodeSettings,
    ) -> Result<(u32, NodeHandle), crate::drive::LayoutError>
    where
        DS: crate::drive::DriveSet,
        DS::Dev: BlockDevice + AsyncRead + Send + 'static,
        L: LivenessLog + Send + 'static,
        F: Fn(usize, &crate::zonestore::RegionZoneStore<DS::Dev>) -> L,
    {
        let (weight, built) = build_multidrive_zoned(
            node_id,
            &driveset,
            shards.len(),
            region_blocks,
            zone_super_blocks,
            zone_region_blocks,
            zone_region_overrides,
            geom,
            create,
        )?;
        let shard_count = shards.len();
        let (maint, maint_join) = crate::maint::spawn_pool_lanes(shard_count, settings.maint_compact_lanes);
        let mut maint = maint.into_iter();
        let mut handle = NodeHandle {
            shards: Vec::new(),
            maint_join,
        };
        let own_shard_addrs: Vec<SocketAddr> =
            shards.iter().filter_map(|l| l.local_addr().ok()).collect();
        if own_shard_addrs.len() != shard_count {
            log::warn!(
                "node {node_id}: resolved only {} of {shard_count} shard addresses; \
                 sorted-zone sibling-shard handoff may be incomplete this run",
                own_shard_addrs.len()
            );
        }
        for (s, (listener, (provider, zone0))) in shards.into_iter().zip(built).enumerate() {
            let peers_s: Vec<(NodeId, SocketAddr)> = peer_shards
                .iter()
                .filter_map(|(p, addrs)| addrs.get(s).map(|&a| (*p, a)))
                .collect();
            let ring_s = ring.clone();
            let log = mk_log(s, &provider);
            let settings = settings.clone();
            let peer_shards_s = peer_shards.clone();
            let own_addrs_s = own_shard_addrs.clone();
            handle.shards.push(spawn_shard_at(
                listener,
                peers_s,
                s,
                sweep_every,
                maint.next().unwrap(),
                peer_shards_s,
                own_addrs_s,
                move || {
                    ServerShared::zoned(
                        node_id,
                        zone0,
                        Rc::new(provider),
                        crate::zone::Catalog::new(),
                        ring_s,
                    )
                    .with_shard(s, shard_count)
                    .with_settings(settings)
                    .with_liveness(gc_grace_ms, Box::new(log))
                },
            ));
        }
        Ok((weight, handle))
    }
}

#[allow(clippy::type_complexity)]
#[allow(clippy::too_many_arguments)]
fn build_multidrive_zoned<DS>(
    node_id: NodeId,
    driveset: &DS,
    num_partitions: usize,
    region_blocks: u64,
    zone_super_blocks: u64,
    zone_region_blocks: u64,
    zone_region_overrides: Vec<(crate::zone::ZoneId, u64)>,
    geom: crate::drive::PartitionGeom,
    create: bool,
) -> Result<
    (
        u32,
        Vec<(
            crate::zonestore::RegionZoneStore<DS::Dev>,
            VersionedStore<crate::zonestore::SubDev<DS::Dev>>,
        )>,
    ),
    crate::drive::LayoutError,
>
where
    DS: crate::drive::DriveSet,
    DS::Dev: BlockDevice + AsyncRead + Send + 'static,
{
    use crate::zonestore::{open_partition_zone_stores, RegionZoneConfig, ZoneStore};
    let caps = crate::drive::drive_caps(driveset);
    let layout = crate::drive::plan(&caps, region_blocks, num_partitions, num_partitions)?;
    let weight = crate::drive::node_weight(driveset.drive_count());
    let zcfg = RegionZoneConfig {
        super_blocks: zone_super_blocks,
        zone_region_blocks,
        zone_region_overrides,
        seg_blocks: geom.seg_blocks,
        reserved_blocks: geom.reserved_blocks,
    };
    let zs_err = |e: crate::zonestore::ZoneStoreError| {
        crate::drive::LayoutError::Io(io::Error::other(e.to_string()))
    };
    let providers = open_partition_zone_stores(driveset, &layout, &zcfg, node_id, create)?;
    let mut built = Vec::with_capacity(num_partitions);
    for provider in providers {
        let zone0 = if create {
            provider.open(DEFAULT_ZONE, crate::zone::Ordering::Unsorted)
        } else {
            provider.open_existing(DEFAULT_ZONE)
        }
        .map_err(zs_err)?;
        built.push((provider, zone0));
    }
    Ok((weight, built))
}

