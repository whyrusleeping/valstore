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
    cleanup_scoped, cleanup_scoped_zone, reconcile_scoped_gate as reconcile_scoped,
    reconcile_scoped_zone_gate as reconcile_scoped_zone, ForgetGate, DEFAULT_BUCKETS,
};
use crate::coord::Transport;
use crate::hlc::physical_now_ms;
use crate::liveness::LivenessLog;
use crate::net::{accept_loop, server_poll, NetTransport, ReadPause, ServerShared};
use crate::reactor::Timer;
use crate::replica::VersionedStore;
use crate::ring::{NodeId, Ring};
use crate::zone::{ZoneId, DEFAULT_ZONE};

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
