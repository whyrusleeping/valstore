---
id: "0009"
title: Slim client crate (coordinator + transport + types only)
status: done
priority: 3
created: "2026-07-02"
updated: "2026-07-04"
depends_on: []
spec_refs: []
---

## Description
BACKLOG.md §Productization: a client embeds all of `vals-cluster` (server included) today. Extract a client-facing crate cutting the surface to coordinator + transport + wire types. 

Note the original prerequisite is largely met: `freeze-contracts` (merged) landed the versioned wire hello (`PROTO_VERSION 1`), on-disk version envelopes, and a sealed public API; `formats.md` remains "snapshot, not frozen" but changes are now gateable on the negotiated version. Non-Rust clients are proven viable (`vals-examples/valsgo` written from the spec alone).

## Acceptance criteria
- A `vals-client` (name TBD) crate a client can depend on without compiling server/PD/store code; `vals-cluster` re-exports or depends on it to avoid duplication.
- Existing examples/getting-started compile against the slim crate.
- Public API surface documented (`#![warn(missing_docs)]` kept).

## Acceptance criteria

## Plan

Extract a client-facing crate `vals-client` from `vals-cluster` so an application can embed coordinator + transport + wire types without compiling server/PD/store code (no vals-store, vals-spdk, vals-raft in its dependency graph). `vals-cluster` depends on `vals-client` and re-exports everything, keeping all existing `vals_cluster::...` paths stable so vals-server/vals-bench need no (or minimal) changes.

## New crate: crates/vals-client
- Cargo deps: `vals-rt` (path), `libc`, `socket2`, `log`. NOTHING else. `#![warn(missing_docs)]`, crate-level docs describing the client surface, plus a quickstart (`no_run` doctest showing NetTransport + Coordinator, or a small mock-Transport doctest).
- Modules MOVED from vals-cluster (keep file contents, adjust `crate::` paths):
  - `hlc.rs` (Hlc, Version, physical_now_ms, pack/unpack_version…). vals-cluster internals (replica.rs etc.) still need pub(crate) items like `unpack_version` — widen those to `#[doc(hidden)] pub` in vals-client; keep root re-exports `Hlc, Version` as the blessed surface.
  - `ring.rs` (Ring, NodeId, hash64).
  - `zone.rs` (Catalog, ZoneConfig, ZoneId, DEFAULT_ZONE, ordering_to_u8/from_u8) — BUT `Ordering` is currently `pub use vals_store::Ordering`; define a new 2-variant `pub enum Ordering { Sorted, Unsorted }` in vals-client's zone module instead. vals-cluster gains one conversion helper (e.g. `fn engine_ordering(zone::Ordering) -> vals_store::Ordering`) used at the engine-creation sites (zonestore.rs / node.rs) and anywhere else the two types meet. No From impls possible in vals-cluster (both types foreign) — a free function is fine.
  - `partition.rs` (RangeMap, RangeSlice).
  - `failure.rs` (FailureDetector, FdConfig).
  - `observe.rs` (log facade + counters; used by moved modules and by everything staying behind — re-export the module).
  - Pure data types out of `replica.rs`: `Stored`, `Applied`, `CasResult`, `EntryPage`, `DigestPage`, `split_stored_blob` (NOT the vals_store-typed aliases PlanPage/SpanPage, NOT VersionedStore) — into e.g. `vals-client/src/types.rs`; vals-cluster's `replica` module re-exports them so `vals_cluster::replica::Stored` and root exports stay valid.
  - `hints.rs` SPLIT: `Hint`, `HintStore` trait, `MemHintStore` → vals-client (coordinator uses them); `FileHintStore` (uses vals_store::crc) STAYS in vals-cluster implementing the vals-client trait.
  - `coord/mod.rs` (Coordinator, Transport, TransportError, CoordError, RingView, ScanPage, BatchEntry/BatchAck/BatchResult, Durability, WriteOutcome, ReadFanout, SloppyAck…) and `coord/service.rs` (CoordinatorService). Decouple `TransportError::Engine(vals_store::EngineError)` → `Engine(String)` (carry the engine error's Display; it is only ever constructed via map_err in antientropy.rs and local_cluster.rs — update those to `.map_err(|e| TransportError::Engine(e.to_string()))` — and never matched for its payload).
  - `coord/local_cluster.rs` STAYS in vals-cluster (uses VersionedStore/vals-spdk). vals-cluster keeps a thin `pub mod coord { pub use vals_client::coord::*; … pub use local_cluster::LocalCluster; }` so `vals_cluster::coord::LocalCluster` and root re-exports still work.
  - `wire.rs`, `proto.rs` → vals-client as `#[doc(hidden)] pub` modules (same status as today). Widen the few pub(crate) helpers (put_sock_addr etc.) to doc-hidden pub where the server (staying behind) needs them.
  - `reactor.rs` (poll_io, Interest, Timer, Notify).
  - `net/client.rs` (NetTransport, NetPeer, CONNECTS) plus the SHARED parts of `net/mod.rs` (frame_msg, FrameRead, catalog_from_wire, MAX_FRAME, MAX_CONN_SEND_BUF, …) → vals-client `net` module; make the server-needed helpers `#[doc(hidden)] pub`. Server-only parts of net/mod.rs (listener plumbing) stay in vals-cluster's `net` wrapper module alongside `server.rs`.
  - Replace `vals_store::index::FxHasher` in client.rs with a small private FxHasher copy in vals-client (~25 lines from vals-store/src/index.rs).
  - The `AePeer` TRAIT (antientropy.rs:209) → vals-client (doc-hidden module, e.g. `pub mod ae` or `antientropy`); its signature only uses client-crate types (ZoneId, TransportError, DigestPage/EntryPage). The `impl AePeer for NetPeer` moves with client.rs. The REST of antientropy.rs stays in vals-cluster and does `pub use vals_client::…::AePeer;` so existing paths keep working.
- vals-client lib.rs: mirror vals-cluster's curated-root-re-export philosophy; documented blessed surface = Coordinator/Transport/errors/Durability/…, NetTransport, Ring/NodeId, zone types, Version/Hlc, RangeMap, Stored/Applied/CasResult, reactor, observe, FailureDetector, hint types. wire/proto/ae stay doc(hidden).

## vals-cluster afterwards
- Cargo: add `vals-client = { path = "../vals-client" }`; keep other deps.
- lib.rs: replace moved `pub mod X;` with `pub use vals_client::X;` module re-exports (zone, ring, reactor, observe, …) or thin wrapper modules where vals-cluster adds pieces (coord adds LocalCluster + tests; net adds server + tests; replica re-exports the moved types; antientropy re-exports AePeer). Keep ALL current root re-exports identical (Coordinator, LocalCluster, NetTransport, Ring, Version, Stored, FileHintStore, MemHintStore, init_cluster_secret, …). auth/liveness/node/pd/drive/zonestore/catalog_store/maint/replica/antientropy/server all stay.
- Tests: `coord/tests.rs` and `net/tests.rs` stay in vals-cluster (they need vals-store/vals-spdk/servers). Where they reach white-box client internals (net/tests.rs line 1: connect_nonblocking, flush_dirty, ConnState, MuxConn, MuxRpc, Waiter), widen those items in vals-client to `#[doc(hidden)] pub` and fix imports. Unit tests living inside moved modules move with them when std-only.
- Workspace Cargo.toml: add `crates/vals-client` to members.

## Docs
- Update vals-cluster lib.rs module map ("this is the crate a SERVER embeds; client apps depend on vals-client").
- docs/getting-started.md: point client applications at `vals-client`.
- Brief note in ARCHITECTURE.md crate list if one exists.

## Acceptance verification
1. `cargo check -p vals-client` compiles standalone; `cargo tree -p vals-client -e normal` shows NO vals-store / vals-spdk / vals-raft.
2. `cargo build --workspace` and `cargo test --workspace` green (pinned 1.96 toolchain).
3. `cargo clippy --workspace` no new warnings; `cargo fmt --check`.
4. `cargo doc -p vals-client` builds with no missing_docs warnings.
5. Doc examples (lib.rs quickstarts) pass as doctests.

### Starting points
- crates/vals-cluster/src/lib.rs — current module map + curated root re-exports to preserve
- coord/mod.rs:36-87 — TransportError::Engine(EngineError) is the only vals-store type in coord; only constructed via map_err in antientropy.rs and coord/local_cluster.rs
- zone.rs:21 — `pub use vals_store::Ordering`; ordering_to_u8/from_u8 nearby; engine-creation conversion sites in zonestore.rs/node.rs
- net/client.rs:34 — vals_store::index::FxHasher (copy ~25 lines from vals-store/src/index.rs:26-51); net/client.rs:725 impl AePeer for NetPeer
- antientropy.rs:209 — trait AePeer signature uses only client-movable types
- replica.rs:91-220 — split_stored_blob, Stored, Applied, CasResult, EntryPage, DigestPage are pure; PlanPage/SpanPage reference vals_store and stay
- net/tests.rs:1-2 — white-box imports from client.rs that need doc(hidden) pub after the move
- hints.rs — FileHintStore uses vals_store::crc::crc32 (stays); Hint/HintStore/MemHintStore move
- vals-rt is std-only (no deps) — safe dependency for vals-client

## Work log
- 2026-07-04 plan: Extract a client-facing crate `vals-client` from `vals-cluster` so an application can embed coordinator + transport + wire types without compiling server/PD/store code (no vals-store, vals-spdk, vals-
…[truncated]
- 2026-07-04 context hints: 9 recorded with plan
- 2026-07-04 context hints: crates/vals-cluster/src/lib.rs — current module map + curated root re-exports to preserve; coord/mod.rs:36-87 — TransportError::Engine(EngineError) is the only vals-store type in coord; constructe
…[truncated]
- 2026-07-04 implementer report: Extracted a new slim `vals-client` crate (coordinator + transport + wire types) from `vals-cluster`, which now depends on and re-exports it so every `vals_cluster::…` path stays stable.  ## New crat
…[truncated]
- 2026-07-04 review tier: high-powered — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change extracts a slim `vals-client` crate (coordinator + transport + wire types) from `vals-cluster` exactly as planned. Verified: `cargo tree -p vals-client -e normal` shows only vals-rt/libc/so
…[truncated]
- 2026-07-04 decision: accept — commit: feat: extract slim vals-client crate (coordinator + transport + wire types) (task 0009)  A client app can now depend on vals-client alone — coordinator (W/R quorum, durability tiers, CAS, batches, s
…[truncated]
- 2026-07-04 usage: 181,037 tok (in 580, out 180,457, cache_r 40,885,333, cache_w 431,328) · cost n/a (unpriced)
  implementer: 147,078 tok (in 486, out 146,592, cache_r 39,327,604, cache_w 263,486) · cost n/a (unpriced)
  coordinator: 27,566 tok (in 58, out 27,508, cache_r 1,203,461, cache_w 131,529) · cost n/a (unpriced)
  reviewer:Claude: 6,393 tok (in 36, out 6,357, cache_r 354,268, cache_w 36,313) · cost n/a (unpriced)
