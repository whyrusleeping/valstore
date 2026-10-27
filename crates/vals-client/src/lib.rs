//! `vals-client`: the slim client-facing crate for the leaderless data path.
//!
//! ```no_run
//! use std::net::SocketAddr;
//! use vals_client::{Coordinator, NetTransport, Ring, WriteConcern};
//!
//! // Address each node's replica server (one shard per node here).
//! let nodes: Vec<(u32, SocketAddr)> = vec![
//!     (1, "127.0.0.1:7001".parse().unwrap()),
//!     (2, "127.0.0.1:7002".parse().unwrap()),
//!     (3, "127.0.0.1:7003".parse().unwrap()),
//! ];
//! let transport = NetTransport::connect(&nodes);
//!
//! // The placement ring: RF = 3, all three nodes, equal weight.
//! let mut ring = Ring::new(3);
//! ring.set_members_weighted([(1, 1), (2, 1), (3, 1)]);
//!
//! // A coordinator: id 100 (distinct from data nodes), strong reads at R = 2.
//! let coord = Coordinator::new(100, ring, 2, transport);
//!
//! vals_rt::block_on(
//!     async {
//!         // Write acknowledged by a majority (W = 2), read it strongly back (R = 2):
//!         // W + R = 4 > RF = 3, so the read must observe the write.
//!         coord
//!             .write_with(b"hello", b"world", WriteConcern::Majority)
//!             .await
//!             .unwrap();
//!         let got = coord.read(b"hello").await.unwrap().expect("present");
//!         assert_eq!(got.value.as_deref(), Some(&b"world"[..]));
//!     },
//!     vals_client::reactor::poll_io, // drive socket readiness
//! );
//! ```

pub mod coord;
pub mod net;
pub mod observe;
pub mod reactor;
pub mod ring;
pub mod types;
pub mod zone;

#[doc(hidden)]
pub mod failure;
#[doc(hidden)]
pub mod hints;
#[doc(hidden)]
pub mod hlc;
#[doc(hidden)]
pub mod partition;

#[doc(hidden)]
pub mod ae;
#[doc(hidden)]
pub mod proto;
#[doc(hidden)]
pub mod wire;

pub use coord::{
    BatchAck, BatchEntry, BatchResult, BatchWriteHandle, CoordError, Coordinator, CoordinatorService,
    HedgeClock, Persistence, ReadFanout, RingView, SloppyAck, Transport, TransportError,
    WriteConcern, WriteOptions, WriteOutcome,
};
pub use failure::{FailureDetector, FdConfig};
pub use hints::{Hint, HintStore, MemHintStore};
pub use hlc::{Hlc, Version};
pub use net::{
    ClientConfig, ClientConfigError, ConnectionStats, NetPeer, NetTransport, CONNECTS,
    DEFAULT_CLIENT_MAX_BYTES, DEFAULT_CLIENT_MAX_OPS, DEFAULT_CLIENT_NODE_MAX_BYTES,
    DEFAULT_CLIENT_NODE_MAX_OPS, DEFAULT_CONNECT_DEADLINE, DEFAULT_RPC_DEADLINE,
};
pub use partition::{
    data_shard, even_boundaries, opaque_placement_token, scan_shard, RangeMap, RangeSlice,
    OPAQUE_TOKEN_LEN, OPAQUE_TOKEN_MAGIC, OPAQUE_TOKEN_SEED_LEN, OPAQUE_TOKEN_VERSION,
};
pub use ring::{NodeId, Ring};
pub use types::{
    split_stored_blob, Applied, CasResult, DigestCursorPage, DigestPage, EntryPage, MixedReadEntry, MixedReadReply,
    MixedReadRole, Stored,
};
pub use zone::{Catalog, Ordering, ZoneConfig, ZoneConfigError, ZoneId, DEFAULT_ZONE};
