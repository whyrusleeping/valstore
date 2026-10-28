//! `vals-client`: the Rust client for a vals cluster.
//!
//! A [`Coordinator`] over a [`NetTransport`] reads and writes keys on a
//! running cluster of `valsd` data nodes (or nodes embedded in another
//! application). The coordinator does the quorum work itself: it picks each
//! key's replicas from the placement [`Ring`], stamps writes with a
//! [`Version`], waits for the requested number of acknowledgements, and merges
//! replica replies by last-writer-wins. This crate contains no storage engine,
//! server or control-plane code; it is not an embedded database. To store data
//! in your own process, see the `vals-store` engine and the `vals-cluster`
//! node and PD crates instead.
//!
//! # Example
//!
//! ```no_run
//! use std::net::SocketAddr;
//! use vals_client::{Coordinator, NetTransport, ReadConcern, Ring, WriteOptions, WriteConcern};
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
//! // A coordinator: id 100 (distinct from data nodes), majority reads.
//! let coord = Coordinator::new(100, ring, ReadConcern::Majority, transport);
//!
//! vals_rt::block_on(
//!     async {
//!         // Write synced to a majority's devices (W = 2), read it back from a
//!         // majority (R = 2): W + R = 4 > RF = 3, so the read observes the write.
//!         let durable = WriteOptions::device_durable(WriteConcern::Majority);
//!         coord.write_with(b"hello", b"world", durable).await.unwrap();
//!         let got = coord.read(b"hello").await.unwrap().expect("present");
//!         assert_eq!(got.value.as_deref(), Some(&b"world"[..]));
//!     },
//!     vals_client::reactor::poll_io, // drive socket readiness
//! );
//! ```
//!
//! The initial ring is only a boot guess. Call [`Coordinator::refresh_ring`]
//! and [`Coordinator::refresh_catalog`] against a live node before traffic to
//! adopt the cluster's current membership, replication factor and zones. The
//! `kv` and `tokio_bridge` examples in this crate are complete programs.
//!
//! # Supported API
//!
//! These items are the supported client surface:
//!
//! - Connecting: [`NetTransport`] (`connect*` constructors), [`ClientConfig`],
//!   [`ClientBudgets`] and [`ClientConfigError`], [`Ring`] and [`NodeId`] for
//!   the boot ring.
//! - Operations: the public `async` methods of [`Coordinator`] (reads,
//!   writes, deletes, batches, CAS, scans, range deletes, routing refresh),
//!   and [`CoordinatorService`] for hosting a coordinator with background
//!   refresh.
//! - Options and results: [`WriteConcern`], [`Persistence`], [`WriteOptions`],
//!   [`ReadConcern`], [`ReadFanout`], [`Stored`], [`Version`], [`CasResult`], [`SloppyAck`],
//!   [`BatchResult`], [`WriteOutcome`], [`ScanQuery`], [`CreditStats`], and
//!   the errors [`CoordError`], [`ReplicaError`], [`RangeDeleteMiss`] and
//!   [`TransportError`].
//! - Zones: [`ZoneId`], [`DEFAULT_ZONE`], [`Catalog`], [`ZoneConfig`],
//!   [`Ordering`].
//! - Driving I/O: [`reactor::poll_io`].
//!
//! Everything else that is public exists so that `vals-cluster`, `valsd` and
//! the benchmarks can share code with the client: the `#[doc(hidden)]`
//! modules and items (wire codec, protocol constants, partition helpers,
//! anti-entropy peers, connection internals), the methods marked hidden on
//! the types above, the [`observe`] counters, and implementing [`Transport`]
//! yourself. Those may change in any revision. There is no API freeze: the
//! crates are not published, so pin a git revision and keep the client on the
//! same revision as the servers it talks to.
//!
//! # Threads and executors
//!
//! A [`Coordinator<NetTransport>`](Coordinator) is neither `Send` nor `Sync`.
//! Its connections register with a thread-local readiness reactor, so build
//! it, and poll every future it returns, on one OS thread, driven by
//! [`vals_rt::block_on`] (or a [`vals_rt::LocalExecutor`]) with
//! [`reactor::poll_io`] as the I/O hook. Another executor that does not call
//! `poll_io` on that thread leaves socket I/O stalled; an empty hook is only
//! enough for in-process transports. Methods take `&self`, so one thread can
//! run many operations at once (for example with [`vals_rt::join_all`]). To
//! scale across cores, run one coordinator per thread; coordinators are
//! independent. From a Tokio application, run the coordinator on a dedicated
//! thread and send it requests over a channel, as the `tokio_bridge` example
//! does.
//!
//! # Failures, retries and cancellation
//!
//! - **A failed write may still have been applied.** When a write or delete
//!   returns an error, any replica that received it may have applied it, and
//!   anti-entropy can later copy it to the others. A timeout, a quorum
//!   shortfall or a lost connection is never proof that nothing changed.
//! - **Retries create new versions.** The client never resends a write on its
//!   own, except that a write refused for a stale ring is retried once, after
//!   the coordinator adopts the current ring, with a fresh version (turn this
//!   off with [`Coordinator::with_manual_refresh`]). If you retry a failed
//!   write, the retry is a new write with a newer version; it overwrites the
//!   earlier attempt if both land, which is safe for idempotent puts and
//!   deletes but not for read-modify-write logic. Reads refused for a stale
//!   ring are retried the same way, and a read on a connection the server
//!   closed while idle is resent once.
//! - **Cancellation is local.** Dropping an operation's future stops waiting
//!   for it and releases its client credits. A request not yet written to the
//!   socket is withdrawn; one already sent may still be applied by its
//!   replica. Dropping a request whose frame was only partly written closes
//!   that connection, which fails the other requests in flight on it.
//! - **Deadlines.** Each RPC has the deadline from [`ClientConfig`] (5 s by
//!   default). An operation that refreshes the ring or waits for client
//!   credits can take longer than one deadline; wrap it in your own timeout if
//!   you need an end-to-end bound.
//!
//! # Persistence and consistency
//!
//! A write succeeds when `W` replicas acknowledge it, where [`WriteConcern`]
//! picks `W`. With [`Persistence::Buffered`] (the default) a replica
//! acknowledges once the write is in its memory, so losing power on every
//! acknowledging replica can lose it; [`Persistence::Device`] waits for each
//! acknowledging replica's device sync. A read waits for `R` replicas, where
//! [`ReadConcern`] picks `R`.
//!
//! Replicas resolve conflicts by last-writer-wins on [`Version`] (a hybrid
//! logical clock plus a random writer id), so the greatest version wins, not
//! the last request to finish. A replica acknowledges a write whose version
//! loses to one it already holds, so a write can succeed and never be
//! visible: if another coordinator whose clock runs ahead wrote the same key
//! moments earlier, the later request carries the smaller version and loses.
//! "Latest" in this documentation always means "greatest version".
//!
//! A read with `W + R > RF` observes the version of any write acknowledged by
//! `W` home replicas, or a greater one (a sloppy write acknowledged by
//! fallback nodes does not count until its hints are delivered). This is not
//! linearizability: there are no transactions, and [`Coordinator::cas`] is a
//! best-effort check-then-write: two racing calls can both succeed, and
//! last-writer-wins then silently keeps only one. Do not build counters,
//! locks or leases, unique-id allocation or balances on it. Scans read each
//! range from `R` replicas page by page; they are not a point-in-time snapshot
//! across pages or ranges.
//!
//! # Choosing a policy
//!
//! For data you cannot afford to lose, write and delete with
//! `WriteOptions::device_durable(WriteConcern::Majority)` and read and scan
//! with [`ReadConcern::Majority`]:
//!
//! - An acknowledged write is on the devices of a majority of its replicas
//!   (as far as each drive's flush contract goes), so it survives every node
//!   losing power at once.
//! - At RF 3, reads and writes each need two of three replicas, so both keep
//!   working with one replica down or unreachable.
//! - A majority write and a majority read overlap at every replication
//!   factor, so the same code stays correct in zones with different RFs.
//!
//! The defaults are different and are kept for compatibility:
//!
//! - [`Coordinator::write`] uses `WriteOptions::default()`, which is
//!   buffered [`WriteConcern::All`]: every replica must acknowledge, so one
//!   unreachable replica fails every write. Any method given a bare
//!   [`WriteConcern`] uses buffered persistence.
//! - [`Coordinator::new`] uses the read concern you pass. A number such as
//!   `2` means [`ReadConcern::Quorum`]`(2)` in every zone, which overlaps
//!   majority writes only where `RF` is at most 4.
//!
//! Buffered majority writes are faster (no device sync per write) and still
//! survive the loss of any minority of replicas; choose them when a
//! simultaneous power loss of a key's replicas is acceptable to you.

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
    BatchResult, BatchWriteHandle, ClientBudgets, CoordError, Coordinator, CoordinatorService,
    CreditStats, Persistence, RangeDeleteMiss, ReadConcern, ReadFanout, ReplicaError, ScanQuery,
    SloppyAck, Transport, TransportError, WriteConcern, WriteOptions, WriteOutcome,
};
#[doc(hidden)]
pub use coord::{BatchAck, BatchEntry, HedgeClock, RingView};
pub use failure::FdConfig;
#[doc(hidden)]
pub use failure::FailureDetector;
pub use hints::{Hint, HintStore, MemHintStore};
pub use hlc::Version;
#[doc(hidden)]
pub use hlc::Hlc;
pub use net::{
    ClientConfig, ClientConfigError, ConnectionStats, NetTransport, DEFAULT_CLIENT_MAX_BYTES,
    DEFAULT_CLIENT_MAX_OPS, DEFAULT_CLIENT_NODE_MAX_BYTES, DEFAULT_CLIENT_NODE_MAX_OPS,
    DEFAULT_CONNECT_DEADLINE, DEFAULT_RPC_DEADLINE,
};
#[doc(hidden)]
pub use net::{NetPeer, CONNECTS};
pub use partition::RangeMap;
#[doc(hidden)]
pub use partition::{
    data_shard, even_boundaries, opaque_placement_token, scan_shard, RangeSlice,
    OPAQUE_TOKEN_LEN, OPAQUE_TOKEN_MAGIC, OPAQUE_TOKEN_SEED_LEN, OPAQUE_TOKEN_VERSION,
};
pub use ring::{NodeId, Ring};
pub use types::{CasResult, Stored};
#[doc(hidden)]
pub use types::{
    split_stored_blob, Applied, DigestCursorPage, DigestPage, EntryPage, MixedReadEntry,
    MixedReadReply, MixedReadRole,
};
pub use zone::{Catalog, Ordering, ZoneConfig, ZoneConfigError, ZoneId, DEFAULT_ZONE};
