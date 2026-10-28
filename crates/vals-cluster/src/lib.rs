//! `vals-cluster`: the leaderless (Dynamo-style) distributed layer.
//!
//! ```
//! use vals_cluster::{Coordinator, LocalCluster, ReadConcern, Ring, VersionedStore, WriteConcern};
//! use vals_spdk::MemBlockDevice;
//! use vals_store::{Config, Engine};
//!
//! // Three replica stores, as a real cluster would hold — one per node.
//! let mut cluster = LocalCluster::new();
//! for node in 1..=3u32 {
//!     let engine = Engine::create(MemBlockDevice::new(4096, 4096), Config::default()).unwrap();
//!     cluster.insert(node, VersionedStore::new(engine, node).unwrap());
//! }
//!
//! // The placement ring: RF = 3, all three nodes, equal weight.
//! let mut ring = Ring::new(3);
//! ring.set_members_weighted([(1, 1), (2, 1), (3, 1)]);
//!
//! // A coordinator: id 100 (distinct from data nodes), majority reads (R = 2).
//! let coord = Coordinator::new(100, ring, ReadConcern::Majority, cluster);
//!
//! // The coordinator API is async; drive it with the bundled executor.
//! vals_rt::block_on(
//!     async {
//!         // Write acknowledged by a majority (W = 2), read it strongly back (R = 2):
//!         // W + R = 4 > RF = 3, so the read must observe the write.
//!         let v = coord
//!             .write_with(b"hello", b"world", WriteConcern::Majority)
//!             .await
//!             .unwrap();
//!         let got = coord.read(b"hello").await.unwrap().expect("present");
//!         assert_eq!(got.value.as_deref(), Some(&b"world"[..]));
//!         assert_eq!(got.version, v);
//!
//!         // Deletes are versioned tombstones — an older write can't resurrect.
//!         coord.delete(b"hello", WriteConcern::Majority).await.unwrap();
//!         assert_eq!(coord.read(b"hello").await.unwrap(), None);
//!     },
//!     || {}, // poll_io hook: nothing to poll in-process
//! );
//! ```
//!
//! # Supported API
//!
//! This crate is for applications that host vals data nodes or the PD
//! (control plane) in their own process; `valsd` is built on it. A program
//! that only talks to a cluster needs `vals-client` instead, which this crate
//! re-exports. The supported embedding surface is:
//!
//! - Data nodes: [`VersionedStore`] (`new` for a freshly created engine,
//!   `open` to recover one), [`Node`] (`serve*` constructors) and
//!   [`node::NodeHandle`], whose `shutdown` drains and stops the node. The
//!   zone store providers [`MemZoneStore`] and [`FileZoneStore`] (and the
//!   SPDK one behind the `spdk` feature), the persistent catalog store, and
//!   the liveness logs [`FileLivenessLog`] and [`MemLivenessLog`] for
//!   tombstone GC.
//! - Control plane: [`PdDaemon`]'s `spawn*_durable*` constructors, its
//!   membership and zone methods, and `shutdown`; [`RingConfig`].
//! - Security: [`init_cluster_secret`], which control-plane traffic needs.
//! - [`LocalCluster`], an in-memory transport for tests and demos only.
//!
//! The lifecycle, persistence, liveness and security rules are in
//! `docs/embedding.md`; the data semantics are those of `vals-client`.
//! [`VersionedStore`] applies versioned writes and deletes (tombstones) on
//! top of a raw `vals-store` engine and is the layer that gives deletes and
//! last-writer-wins their meaning; do not mix raw engine writes into a store
//! it owns.
//!
//! The many other public modules and items (anti-entropy, backup and dump
//! internals, drive layout, PD internals, server plumbing, test helpers) are
//! shared with `valsd`, the tools and the tests. They may change in any
//! revision. The crates are not published and there is no API freeze; keep
//! every vals crate, and the servers you talk to, on one revision.

pub mod observe;
pub use vals_client::{reactor, ring, zone};
#[doc(hidden)]
pub use vals_client::{proto, wire};
pub(crate) use vals_client::{failure, hlc, partition};

pub mod backup;
pub mod catalog_store;
pub mod coord;
pub mod drive;
pub mod dump;
pub mod integrity;
pub mod keyscan;
pub mod liveness;
pub mod net;
pub mod node;
pub mod node_settings;
pub mod pd;
pub mod replica;
pub mod transport;
pub mod zonestore;

pub(crate) mod auth;
pub(crate) mod hints;
pub(crate) mod maint;
pub(crate) mod ram;
pub(crate) mod ram_broker;
pub(crate) mod rt_sync;
#[cfg(test)]
mod rt_sync_tests;

#[cfg(test)]
pub(crate) mod testutil;

#[doc(hidden)]
pub mod antientropy;

pub use auth::init_cluster_secret;
pub use coord::{
    BatchAck, BatchEntry, BatchResult, CoordError, Coordinator, CoordinatorService, CreditStats,
    LocalCluster, Persistence, ReadConcern, ReadFanout, RingView, SloppyAck, Transport,
    TransportError, WriteConcern, WriteOptions, WriteOutcome,
};
pub use drive::{
    build_stores, build_stores_with, drive_caps, node_weight, plan, relocate, relocate_with,
    replan, DriveSet, Layout, LayoutError, MemDriveSet, Partition, PartitionGeom,
};
pub use failure::{FailureDetector, FdConfig};
pub use hints::{FileHintStore, Hint, HintStore, MemHintStore};
pub use hlc::{Hlc, Version};
pub use liveness::{
    is_fenced, rejoin_decision, FileLivenessLog, LivenessLog, MemLivenessLog, RejoinDecision,
    WipeStats,
};
pub use net::{
    ClientConfig, ClientConfigError, ConnectionStats, NetTransport, ReplicaServer, ServerHandle,
    ShardedServer, DEFAULT_CLIENT_MAX_BYTES, DEFAULT_CLIENT_MAX_OPS, DEFAULT_CLIENT_NODE_MAX_BYTES,
    DEFAULT_CLIENT_NODE_MAX_OPS, DEFAULT_CONNECT_DEADLINE, DEFAULT_RPC_DEADLINE,
};
pub use node::Node;
pub use partition::{
    data_shard, even_boundaries, opaque_placement_token, scan_shard, RangeMap, RangeSlice,
    OPAQUE_TOKEN_LEN, OPAQUE_TOKEN_MAGIC, OPAQUE_TOKEN_SEED_LEN, OPAQUE_TOKEN_VERSION,
};
pub use pd::{PdDaemon, PlacementDriver, RingConfig};
pub use replica::{
    split_stored_blob, Applied, CasResult, StagedApply, StagedApplyOutcome, Stored, VersionedStore,
};
pub use ring::{NodeId, Ring};
pub use zone::{Catalog, Ordering, ZoneConfig, ZoneConfigError, ZoneId, DEFAULT_ZONE};
pub use zonestore::{
    FileZoneStore, MemZoneStore, RegionTable, RegionZoneConfig, RegionZoneStore, SubDev,
    SuperblockStore, ZoneGeom, ZoneRegion, ZoneStore, ZoneStoreError,
};

pub(crate) fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
