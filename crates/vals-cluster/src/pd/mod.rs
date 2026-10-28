use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{self, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use std::collections::BTreeSet;

pub use vals_raft::PartitionError;
use vals_raft::{Command, CommitResolution, Message, Partition};
use vals_spdk::BlockDevice;
use vals_store::Engine;

use crate::partition::RangeMap;
use crate::ring::{NodeId, Ring};
use crate::wire::{self, Request, Response, WireRangeMap, WireZone};
use crate::zone::{ordering_from_u8, ordering_to_u8, Catalog, ZoneConfig, ZoneId, DEFAULT_ZONE};

mod balance;
mod cleanup_gate;
mod daemon;
mod driver;
mod push;
mod range_policy;
mod settings;
mod snapshot;
mod state;

use balance::*;
use daemon::*;
use driver::*;
use push::*;
use state::*;

pub use cleanup_gate::{check_membership_change, MembershipChange};
pub use driver::{AddressedMembers, PlacementDriver, RingConfig};
pub use settings::{PdPlacementOverrides, PdPlacementSettings};
pub use snapshot::{
    placement_equivalent, restore_seed_of, restored_state, seed_voters_entry, view_state, without_pending,
    Lineage, PdStateView, RestoreSeed,
};
pub use state::{AdminMutation, AdminOutcome, PdDaemon, PdInspection, PdStatus, SplitMode};
#[cfg(test)]
pub(crate) use snapshot::seed_ack_entry;
pub(crate) use balance::{BalanceMapUpgrade, BalancePollAuthority, BalanceProposal};
#[cfg(test)]
pub(crate) use range_policy::range_stats_poll;
pub(crate) use range_policy::{fetch_range_stats, range_stats_apply, RangeStatsFetch};
pub(crate) use state::{BalanceExecutorState, PdCmd, PushState, RecoveryHold, RehomeReportState};

#[cfg(test)]
mod tests;
