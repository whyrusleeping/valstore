use std::cell::Cell;
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use vals_rt::{Either, LocalExecutor};
use vals_spdk::{AsyncRead, BlockDevice};
use vals_store::EngineError;

use crate::antientropy::{
    reconcile_scoped_gate as reconcile_scoped,
    reconcile_scoped_zone_gate as reconcile_scoped_zone, ForgetGate, DEFAULT_BUCKETS,
};
#[cfg(test)]
use crate::antientropy::cleanup_scoped;
use crate::coord::Transport;
use crate::hlc::physical_now_ms;
use crate::liveness::LivenessLog;
use crate::net::{accept_loop, server_poll, NetTransport, ReadPause, ServerShared};
use crate::reactor::Timer;
use crate::replica::VersionedStore;
use crate::ring::{NodeId, Ring};
use crate::zone::{ZoneId, DEFAULT_ZONE};

/// Constructors that start a data node serving the vals protocol.
///
/// Each `serve*` call spawns the node's shard threads (each with its own
/// executor and reactor) and maintenance threads, starts accepting on the
/// given nonblocking listeners, and returns a [`NodeHandle`]. The boot ring
/// and peers seed startup; under a PD the node adopts the PD's membership
/// and zones when they are pushed. Call [`NodeHandle::shutdown`] before the
/// process exits.
pub struct Node;


mod control;
mod maintenance;
mod rehome;
mod runtime;
mod start;
mod sweep;

use control::ShardHandle;
use maintenance::*;
#[cfg(test)]
pub(crate) use maintenance::finish_cover_scan_chunk;
use rehome::*;
use runtime::*;
use sweep::*;

pub use control::{NodeCtl, NodeDumper, NodeHandle, NodeRebalancer, ZoneSegmentStats};
pub(crate) use runtime::shutdown_loop;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod seed_oracle_tests;
#[cfg(test)]
mod integrity_tests;
#[cfg(test)]
mod merge_stress_tests;
#[cfg(test)]
mod wipe_tests;
#[cfg(test)]
mod former_owner_tests;
#[cfg(test)]
mod seed_reap_tests;
