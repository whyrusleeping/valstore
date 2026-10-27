
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use raft::prelude::*;
use raft::StateRole;
use vals_spdk::BlockDevice;
use vals_store::{Engine, EngineError};

mod storage;
pub use storage::PdStorage;

pub use raft::prelude::{ConfChangeType, ConfState, Message};

pub fn encode_message(msg: &Message) -> Vec<u8> {
    use protobuf::Message as _;
    msg.write_to_bytes().unwrap_or_default()
}

pub fn decode_message(bytes: &[u8]) -> Option<Message> {
    use protobuf::Message as _;
    Message::parse_from_bytes(bytes).ok()
}

struct SlogToLog;

impl slog::Drain for SlogToLog {
    type Ok = ();
    type Err = slog::Never;

    fn log(
        &self,
        record: &slog::Record,
        _values: &slog::OwnedKVList,
    ) -> Result<Self::Ok, Self::Err> {
        let level = match record.level() {
            slog::Level::Critical | slog::Level::Error => log::Level::Error,
            slog::Level::Warning => log::Level::Warn,
            slog::Level::Info => log::Level::Info,
            slog::Level::Debug => log::Level::Debug,
            slog::Level::Trace => log::Level::Trace,
        };
        log::log!(target: "raft", level, "{}", record.msg());
        Ok(())
    }
}

fn raft_logger() -> slog::Logger {
    use slog::Drain as _;
    slog::Logger::root(SlogToLog.fuse(), slog::o!())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    ConditionalWrite {
        checks: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    },
    ConditionalBatch {
        checks: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        writes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    },
}

impl Command {
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        match self {
            Command::Put(k, v) => {
                b.push(0);
                put_field(&mut b, k);
                put_field(&mut b, v);
            }
            Command::Delete(k) => {
                b.push(1);
                put_field(&mut b, k);
            }
            Command::ConditionalWrite {
                checks,
                key,
                value,
            } => {
                b.push(2);
                put_checks(&mut b, checks);
                put_field(&mut b, key);
                put_optional_field(&mut b, value.as_deref());
            }
            Command::ConditionalBatch { checks, writes } => {
                b.push(3);
                put_checks(&mut b, checks);
                b.extend_from_slice(&(writes.len() as u32).to_le_bytes());
                for (key, value) in writes {
                    put_field(&mut b, key);
                    put_optional_field(&mut b, value.as_deref());
                }
            }
        }
        b
    }

    pub fn decode(data: &[u8]) -> Option<Command> {
        let (&tag, mut rest) = data.split_first()?;
        let cmd = match tag {
            0 => {
                let key = get_field(&mut rest)?;
                let val = get_field(&mut rest)?;
                Command::Put(key, val)
            }
            1 => Command::Delete(get_field(&mut rest)?),
            2 => {
                let checks = get_checks(&mut rest)?;
                let key = get_field(&mut rest)?;
                let value = get_optional_field(&mut rest)?;
                Command::ConditionalWrite { checks, key, value }
            }
            3 => {
                let checks = get_checks(&mut rest)?;
                let mut writes = Vec::new();
                for _ in 0..get_u32(&mut rest)? {
                    writes.push((get_field(&mut rest)?, get_optional_field(&mut rest)?));
                }
                Command::ConditionalBatch { checks, writes }
            }
            _ => return None,
        };
        rest.is_empty().then_some(cmd)
    }
}

const STATE_ENTRY_OVERHEAD: usize = 32;

fn state_entry_bytes(key: &[u8], value: &[u8]) -> usize {
    key.len()
        .saturating_add(value.len())
        .saturating_add(STATE_ENTRY_OVERHEAD)
}

fn command_gross_growth(command: &Command) -> usize {
    match command {
        Command::Put(key, value) => state_entry_bytes(key, value),
        Command::Delete(_) => 0,
        Command::ConditionalWrite { key, value, .. } => value
            .as_ref()
            .map_or(0, |value| state_entry_bytes(key, value)),
        Command::ConditionalBatch { writes, .. } => writes.iter().fold(0usize, |total, (key, value)| {
            total.saturating_add(value.as_ref().map_or(0, |value| state_entry_bytes(key, value)))
        }),
    }
}

fn put_field(b: &mut Vec<u8>, f: &[u8]) {
    b.extend_from_slice(&(f.len() as u32).to_le_bytes());
    b.extend_from_slice(f);
}

fn put_optional_field(b: &mut Vec<u8>, value: Option<&[u8]>) {
    b.push(u8::from(value.is_some()));
    if let Some(value) = value {
        put_field(b, value);
    }
}

fn put_checks(b: &mut Vec<u8>, checks: &[(Vec<u8>, Option<Vec<u8>>)]) {
    b.extend_from_slice(&(checks.len() as u32).to_le_bytes());
    for (key, expected) in checks {
        put_field(b, key);
        put_optional_field(b, expected.as_deref());
    }
}

fn get_checks(rest: &mut &[u8]) -> Option<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
    let mut checks = Vec::new();
    for _ in 0..get_u32(rest)? {
        checks.push((get_field(rest)?, get_optional_field(rest)?));
    }
    Some(checks)
}

fn dump_engine<D: BlockDevice>(engine: &Engine<D>) -> Result<Vec<u8>, PartitionError> {
    let mut out = Vec::new();
    for key in engine.keys() {
        if let Some(value) = engine.get(&key)? {
            put_field(&mut out, &key);
            put_field(&mut out, &value);
        }
    }
    Ok(out)
}

fn restore_engine<D: BlockDevice>(
    engine: &mut Engine<D>,
    data: &[u8],
) -> Result<(), PartitionError> {
    for key in engine.keys() {
        engine.delete(&key, 0)?;
    }
    let mut rest = data;
    while !rest.is_empty() {
        let (Some(key), Some(value)) = (get_field(&mut rest), get_field(&mut rest)) else {
            return Err(PartitionError::Storage(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "malformed snapshot data",
            )));
        };
        engine.put(&key, &value, 0)?;
    }
    Ok(())
}

struct BoundedState {
    values: HashMap<Vec<u8>, Vec<u8>>,
    used: usize,
    capacity: usize,
}

impl BoundedState {
    fn new(capacity: usize) -> Self {
        BoundedState { values: HashMap::new(), used: 0, capacity }
    }

    fn restore(&mut self, data: &[u8]) -> Result<(), PartitionError> {
        let mut values = HashMap::new();
        let mut used = 0usize;
        let mut rest = data;
        while !rest.is_empty() {
            let (Some(key), Some(value)) = (get_field(&mut rest), get_field(&mut rest)) else {
                return Err(invalid_state("malformed snapshot data"));
            };
            used = used.saturating_add(state_entry_bytes(&key, &value));
            values.insert(key, value);
        }
        if used > self.capacity {
            return Err(PartitionError::CapacityExceeded {
                capacity: self.capacity,
                used: 0,
                reserved: 0,
                requested: used,
            });
        }
        self.values = values;
        self.used = used;
        Ok(())
    }

    fn dump(&self) -> Vec<u8> {
        let mut entries: Vec<_> = self.values.iter().collect();
        entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
        let mut out = Vec::with_capacity(self.used + entries.len() * 8);
        for (key, value) in entries {
            put_field(&mut out, key);
            put_field(&mut out, value);
        }
        out
    }

    fn apply(&mut self, writes: &[(Vec<u8>, Option<Vec<u8>>)]) -> Result<(), PartitionError> {
        let mut final_writes = HashMap::new();
        for (key, value) in writes {
            final_writes.insert(key.as_slice(), value.as_deref());
        }
        let mut used = self.used;
        for (key, value) in final_writes {
            if let Some(old) = self.values.get(key) {
                used = used.saturating_sub(state_entry_bytes(key, old));
            }
            if let Some(value) = value {
                used = used.saturating_add(state_entry_bytes(key, value));
            }
        }
        if used > self.capacity {
            return Err(PartitionError::CapacityExceeded {
                capacity: self.capacity,
                used: self.used,
                reserved: 0,
                requested: used.saturating_sub(self.used),
            });
        }
        for (key, value) in writes {
            match value {
                Some(value) => {
                    self.values.insert(key.clone(), value.clone());
                }
                None => {
                    self.values.remove(key);
                }
            }
        }
        self.used = used;
        Ok(())
    }
}

fn invalid_state(message: &'static str) -> PartitionError {
    PartitionError::Storage(std::io::Error::new(std::io::ErrorKind::InvalidData, message))
}

fn get_optional_tag(rest: &mut &[u8]) -> Option<bool> {
    let (&tag, tail) = rest.split_first()?;
    *rest = tail;
    match tag {
        0 => Some(false),
        1 => Some(true),
        _ => None,
    }
}

fn get_optional_field(rest: &mut &[u8]) -> Option<Option<Vec<u8>>> {
    if get_optional_tag(rest)? {
        Some(Some(get_field(rest)?))
    } else {
        Some(None)
    }
}

fn get_u32(rest: &mut &[u8]) -> Option<u32> {
    let (bytes, tail) = rest.split_at_checked(4)?;
    *rest = tail;
    Some(u32::from_le_bytes(bytes.try_into().ok()?))
}

fn get_field(rest: &mut &[u8]) -> Option<Vec<u8>> {
    if rest.len() < 4 {
        return None;
    }
    let (len_bytes, after_len) = rest.split_at(4);
    let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
    if after_len.len() < len {
        return None;
    }
    let (field, after) = after_len.split_at(len);
    *rest = after;
    Some(field.to_vec())
}

#[derive(Debug)]
pub enum PartitionError {
    Raft(raft::Error),
    Engine(EngineError),
    Storage(std::io::Error),
    CapacityExceeded {
        capacity: usize,
        used: usize,
        reserved: usize,
        requested: usize,
    },
    Invariant(String),
    NotLeader,
    NotReady,
    Superseded { index: u64, term: u64, committed_term: u64 },
    AdminQueueExpired,
    AdminNoReply,
    Unknown { index: u64, term: u64 },
    RestartRequired,
}

impl std::fmt::Display for PartitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PartitionError::Raft(e) => write!(f, "raft error: {e}"),
            PartitionError::Engine(e) => write!(f, "engine error: {e}"),
            PartitionError::Storage(e) => write!(f, "raft storage error: {e}"),
            PartitionError::CapacityExceeded { capacity, used, reserved, requested } => write!(
                f,
                "PD state capacity exhausted: capacity {capacity} bytes, used {used}, \
                 reserved {reserved}, requested {requested}"
            ),
            PartitionError::Invariant(message) => write!(f, "PD invariant violation: {message}"),
            PartitionError::NotLeader => {
                write!(f, "raft proposal requires the current leader; submit there and retry")
            }
            PartitionError::NotReady => write!(f, "raft leader is not caught up; retry proposal"),
            PartitionError::Superseded { index, term, committed_term } => write!(f,
                "not committed: index={index} term={term} slot committed by term {committed_term}; nothing applied"),
            PartitionError::AdminQueueExpired => write!(f,
                "timed out in PD queue before proposal; nothing applied"),
            PartitionError::AdminNoReply => write!(f,
                "no reply from PD drive loop; inspect authoritative leader before retrying"),
            PartitionError::Unknown { index, term } => write!(f, "commit not observed for index={index} term={term}; inspect current leader before retrying"),
            PartitionError::RestartRequired => {
                write!(f, "raft partition failed; restart required")
            }
        }
    }
}
impl std::error::Error for PartitionError {}
impl From<raft::Error> for PartitionError {
    fn from(e: raft::Error) -> Self {
        PartitionError::Raft(e)
    }
}
impl From<EngineError> for PartitionError {
    fn from(e: EngineError) -> Self {
        PartitionError::Engine(e)
    }
}
impl From<std::io::Error> for PartitionError {
    fn from(e: std::io::Error) -> Self {
        PartitionError::Storage(e)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitResolution {
    Committed,
    Superseded(u64),
    Unknown,
}

const DEFAULT_SNAPSHOT_INTERVAL: u64 = 64;
const STATE_CHANGE_LOG_LIMIT: usize = 1 << 16;

pub struct Partition<D: BlockDevice> {
    node: RawNode<PdStorage>,
    storage: PdStorage,
    engine: Option<Engine<D>>,
    bounded: Option<BoundedState>,
    applied: u64,
    snapshot_interval: u64,
    outbox: Vec<Message>,
    read_states: Vec<ReadState>,
    commit_watches: BTreeMap<(u64, u64), ()>,
    resolved_commits: Vec<(u64, u64, CommitResolution)>,
    reservations: BTreeMap<u64, usize>,
    reserved: usize,
    failed: bool,
    joining: bool,
    conf_version: u64,
    force_snapshot: bool,
    state_changes: Vec<Vec<u8>>,
    state_changes_lost: bool,
    #[cfg(test)]
    fail_apply_after: Option<usize>,
}

impl<D: BlockDevice> Partition<D> {
    pub fn new(id: u64, voters: Vec<u64>, engine: Engine<D>) -> Result<Self, PartitionError> {
        Self::with_storage(id, PdStorage::in_memory(voters), engine)
    }

    pub fn open(
        id: u64,
        voters: Vec<u64>,
        mut engine: Engine<D>,
        dir: impl AsRef<Path>,
    ) -> Result<Self, PartitionError> {
        let storage = PdStorage::open(dir, voters)?;
        restore_engine(&mut engine, &storage.snapshot_data())?;
        Self::with_storage(id, storage, engine)
    }

    fn with_storage(
        id: u64,
        storage: PdStorage,
        engine: Engine<D>,
    ) -> Result<Self, PartitionError> {
        Self::with_state(id, storage, Some(engine), None)
    }

    fn with_state(
        id: u64,
        storage: PdStorage,
        engine: Option<Engine<D>>,
        bounded: Option<BoundedState>,
    ) -> Result<Self, PartitionError> {
        let applied = storage.snapshot_index();
        let cfg = Config {
            id,
            election_tick: 10,
            heartbeat_tick: 3,
            pre_vote: true,
            check_quorum: true,
            applied,
            ..Default::default()
        };
        cfg.validate()?;
        let logger = raft_logger();
        let node = RawNode::new(&cfg, storage.clone(), &logger)?;
        if storage.is_joining() && storage.snapshot_index() > 0 {
            storage.finish_join()?;
        }
        let joining = storage.is_joining();
        Ok(Partition {
            node,
            storage,
            engine,
            bounded,
            applied,
            snapshot_interval: DEFAULT_SNAPSHOT_INTERVAL,
            outbox: Vec::new(),
            read_states: Vec::new(),
            commit_watches: BTreeMap::new(),
            resolved_commits: Vec::new(),
            reservations: BTreeMap::new(),
            reserved: 0,
            failed: false,
            joining,
            conf_version: 0,
            force_snapshot: false,
            state_changes: Vec::new(),
            state_changes_lost: true,
            #[cfg(test)]
            fail_apply_after: None,
        })
    }

    pub fn with_snapshot_interval(mut self, every: u64) -> Self {
        self.snapshot_interval = every.max(1);
        self
    }

    pub fn state_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, PartitionError> {
        self.ensure_healthy()?;
        match (&self.engine, &self.bounded) {
            (Some(engine), None) => Ok(engine.get(key)?),
            (None, Some(state)) => Ok(state.values.get(key).cloned()),
            _ => unreachable!(),
        }
    }

    pub fn state_keys(&self) -> Vec<Vec<u8>> {
        self.assert_healthy();
        match (&self.engine, &self.bounded) {
            (Some(engine), None) => engine.keys(),
            (None, Some(state)) => state.values.keys().cloned().collect(),
            _ => unreachable!(),
        }
    }

    pub fn state_capacity(&self) -> Option<usize> {
        self.bounded.as_ref().map(|state| state.capacity)
    }

    pub fn state_usage(&self) -> usize {
        self.bounded.as_ref().map_or(0, |state| state.used)
    }

    pub fn state_reserved(&self) -> usize {
        self.reserved
    }

    fn command_live_growth(&self, command: &Command) -> usize {
        let writes: Vec<(&[u8], Option<&[u8]>)> = match command {
            Command::Put(key, value) => vec![(key, Some(value))],
            Command::Delete(key) => vec![(key, None)],
            Command::ConditionalWrite { key, value, .. } => {
                vec![(key, value.as_deref())]
            }
            Command::ConditionalBatch { writes, .. } => writes
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_deref()))
                .collect(),
        };
        let Some(state) = self.bounded.as_ref() else { return 0 };
        let mut final_writes = HashMap::new();
        for (key, value) in writes {
            final_writes.insert(key, value);
        }
        let mut projected = state.used;
        for (key, value) in final_writes {
            if let Some(old) = state.values.get(key) {
                projected = projected.saturating_sub(state_entry_bytes(key, old));
            }
            if let Some(value) = value {
                projected = projected.saturating_add(state_entry_bytes(key, value));
            }
        }
        projected.saturating_sub(state.used)
    }

    fn dump_state(&self) -> Result<Vec<u8>, PartitionError> {
        match (&self.engine, &self.bounded) {
            (Some(engine), None) => dump_engine(engine),
            (None, Some(state)) => Ok(state.dump()),
            _ => unreachable!(),
        }
    }

    pub fn take_state_changes(&mut self) -> Option<Vec<Vec<u8>>> {
        let keys = std::mem::take(&mut self.state_changes);
        (!std::mem::take(&mut self.state_changes_lost)).then_some(keys)
    }

    fn note_state_writes(&mut self, writes: &[(Vec<u8>, Option<Vec<u8>>)]) {
        if self.state_changes_lost {
            return;
        }
        if self.state_changes.len().saturating_add(writes.len()) > STATE_CHANGE_LOG_LIMIT {
            self.state_changes = Vec::new();
            self.state_changes_lost = true;
            return;
        }
        self.state_changes.extend(writes.iter().map(|(key, _)| key.clone()));
    }

    fn restore_state(&mut self, data: &[u8]) -> Result<(), PartitionError> {
        self.state_changes = Vec::new();
        self.state_changes_lost = true;
        match (&mut self.engine, &mut self.bounded) {
            (Some(engine), None) => restore_engine(engine, data),
            (None, Some(state)) => state.restore(data),
            _ => unreachable!(),
        }
    }

    pub fn snapshot_index(&self) -> u64 {
        self.assert_healthy();
        self.storage.snapshot_index()
    }

    fn ensure_healthy(&self) -> Result<(), PartitionError> {
        if self.failed {
            Err(PartitionError::RestartRequired)
        } else {
            Ok(())
        }
    }

    fn assert_healthy(&self) {
        assert!(!self.failed, "raft partition failed; restart required");
    }

    pub fn new_single(id: u64, engine: Engine<D>) -> Result<Self, PartitionError> {
        let mut p = Self::new(id, vec![id], engine)?;
        p.node.campaign()?;
        Ok(p)
    }

    pub fn campaign(&mut self) -> Result<(), PartitionError> {
        self.ensure_healthy()?;
        self.node.campaign()?;
        Ok(())
    }

    pub fn step(&mut self, msg: Message) -> Result<(), PartitionError> {
        self.ensure_healthy()?;
        if let Some(message) = self.recreated_member(&msg) {
            log::error!("raft {}: {message}", self.node.raft.id);
            self.outbox.clear();
            self.failed = true;
            return Err(PartitionError::Invariant(message));
        }
        if !self.accepts_from(&msg) {
            return Ok(());
        }
        self.node.step(msg)?;
        Ok(())
    }

    fn recreated_member(&self, msg: &Message) -> Option<String> {
        if !self.joining {
            return None;
        }
        let id = self.node.raft.id;
        let evidence = match msg.get_msg_type() {
            MessageType::MsgHeartbeat if msg.commit > self.node.raft.raft_log.last_index() => format!(
                "leader {} already tracks it as a member holding the log through index {}",
                msg.from, msg.commit
            ),
            MessageType::MsgAppend if msg.index == 0 => format!(
                "leader {} sent it the raft log from index 1, which a leader only does for a \
                 founding member (a new learner always gets a snapshot)",
                msg.from
            ),
            _ => return None,
        };
        Some(format!(
            "this empty data directory joined as raft id {id}, but {evidence}: a lost member's id \
             cannot be recreated with empty state; stop this replica and replace the voter with a \
             new id"
        ))
    }

    fn accepts_from(&self, msg: &Message) -> bool {
        let kind = msg.get_msg_type();
        let vote = matches!(kind, MessageType::MsgRequestVote | MessageType::MsgRequestPreVote);
        let prs = self.node.raft.prs();
        if prs.iter().next().is_none() {
            return !vote && kind != MessageType::MsgTimeoutNow;
        }
        if prs.get(msg.from).is_some() {
            return true;
        }
        match kind {
            MessageType::MsgRequestVote | MessageType::MsgRequestPreVote => {
                self.node.raft.raft_log.is_up_to_date(msg.index, msg.log_term)
            }
            MessageType::MsgTimeoutNow
            | MessageType::MsgAppendResponse
            | MessageType::MsgRequestVoteResponse
            | MessageType::MsgHeartbeatResponse
            | MessageType::MsgRequestPreVoteResponse
            | MessageType::MsgUnreachable => false,
            _ => true,
        }
    }

    pub fn take_outgoing(&mut self) -> Vec<Message> {
        if self.failed {
            self.outbox.clear();
            Vec::new()
        } else {
            std::mem::take(&mut self.outbox)
        }
    }

    pub fn watch_commit(&mut self, index: u64, term: u64) {
        self.assert_healthy();
        self.commit_watches.insert((index, term), ());
    }

    pub fn unwatch_commit(&mut self, index: u64, term: u64) {
        self.commit_watches.remove(&(index, term));
    }

    pub fn take_resolved_commits(&mut self) -> Vec<(u64, u64, CommitResolution)> {
        std::mem::take(&mut self.resolved_commits)
    }

    pub fn read_index(&mut self, ctx: Vec<u8>) -> Result<(), PartitionError> {
        self.ensure_healthy()?;
        self.node.read_index(ctx);
        Ok(())
    }

    pub fn take_read_states(&mut self) -> Vec<ReadState> {
        std::mem::take(&mut self.read_states)
    }

    pub fn propose(&mut self, cmd: &Command) -> Result<(), PartitionError> {
        self.propose_index(cmd).map(|_| ())
    }

    pub fn propose_index(&mut self, cmd: &Command) -> Result<u64, PartitionError> {
        self.ensure_healthy()?;
        let requested = self.admit(cmd)?;
        let before = self.node.raft.raft_log.last_index();
        self.node.propose(vec![], cmd.encode())?;
        let index = self.node.raft.raft_log.last_index();
        self.reserve(before, index, requested);
        Ok(index)
    }

    fn admit(&self, cmd: &Command) -> Result<usize, PartitionError> {
        let requested = if self.bounded.is_some() && self.reservations.is_empty() {
            self.command_live_growth(cmd)
        } else {
            command_gross_growth(cmd)
        };
        if let Some(capacity) = self.state_capacity() {
            let used = self.state_usage();
            if used.saturating_add(self.reserved).saturating_add(requested) > capacity {
                return Err(PartitionError::CapacityExceeded {
                    capacity,
                    used,
                    reserved: self.reserved,
                    requested,
                });
            }
        }
        Ok(requested)
    }

    fn reserve(&mut self, before: u64, index: u64, requested: usize) {
        if index > before && self.bounded.is_some() {
            *self.reservations.entry(index).or_default() += requested;
            self.reserved = self.reserved.saturating_add(requested);
        }
    }

    pub fn propose_conf_change(
        &mut self,
        change: ConfChangeType,
        id: u64,
        context: Option<&Command>,
    ) -> Result<u64, PartitionError> {
        self.ensure_healthy()?;
        if self.node.raft.state != StateRole::Leader {
            return Err(PartitionError::NotLeader);
        }
        if self.node.raft.has_pending_conf() {
            return Err(PartitionError::NotReady);
        }
        let requested = match context {
            Some(cmd) => self.admit(cmd)?,
            None => 0,
        };
        let before = self.node.raft.raft_log.last_index();
        let mut cc = ConfChange::default();
        cc.set_change_type(change);
        cc.node_id = id;
        self.node.propose_conf_change(context.map(Command::encode).unwrap_or_default(), cc)?;
        let index = self.node.raft.raft_log.last_index();
        if index == before || self.node.raft.pending_conf_index != index {
            return Err(PartitionError::Invariant(format!(
                "raft did not accept the membership change of {id} (index {index}); nothing to commit"
            )));
        }
        self.reserve(before, index, requested);
        Ok(index)
    }

    pub fn has_pending_conf(&self) -> bool {
        self.assert_healthy();
        self.node.raft.has_pending_conf()
    }

    pub fn conf_state(&self) -> ConfState {
        self.assert_healthy();
        self.node.raft.prs().conf().to_conf_state()
    }

    pub fn conf_version(&self) -> u64 {
        self.conf_version
    }

    pub fn is_joining(&self) -> bool {
        self.joining
    }

    pub fn peer_matched(&self, id: u64) -> Option<u64> {
        self.assert_healthy();
        self.node.raft.prs().get(id).map(|pr| pr.matched)
    }

    pub fn force_campaign(&mut self) -> Result<(), PartitionError> {
        self.ensure_healthy()?;
        let id = self.node.raft.id;
        let mut timeout_now = Message::default();
        timeout_now.set_msg_type(MessageType::MsgTimeoutNow);
        timeout_now.from = id;
        timeout_now.to = id;
        self.node.step(timeout_now)?;
        Ok(())
    }

    pub fn tick(&mut self) {
        self.assert_healthy();
        self.node.tick();
    }

    pub fn drive(&mut self) -> Result<(), PartitionError> {
        self.ensure_healthy()?;
        if let Err(error) = self.drive_ready() {
            self.outbox.clear();
            self.failed = true;
            return Err(error);
        }
        Ok(())
    }

    fn drive_ready(&mut self) -> Result<(), PartitionError> {
        if !self.node.has_ready() {
            return Ok(());
        }
        let mut ready = self.node.ready();

        self.outbox.append(&mut ready.take_messages());
        self.read_states.extend(ready.take_read_states());

        if ready.snapshot().get_metadata().index != 0 {
            self.install_snapshot(ready.snapshot().clone())?;
        }
        self.apply(ready.take_committed_entries())?;
        if !ready.entries().is_empty() {
            self.storage.append(ready.entries())?;
        }
        if let Some(hs) = ready.hs() {
            self.storage.set_hardstate(hs.clone())?;
        }
        self.outbox.append(&mut ready.take_persisted_messages());

        let mut light = self.node.advance(ready);
        if let Some(commit) = light.commit_index() {
            self.storage.set_commit(commit)?;
        }
        self.outbox.append(&mut light.take_messages());
        self.apply(light.take_committed_entries())?;
        self.node.advance_apply();
        self.maybe_snapshot()?;
        Ok(())
    }

    fn install_snapshot(&mut self, snapshot: Snapshot) -> Result<(), PartitionError> {
        let index = snapshot.get_metadata().index;
        let term = snapshot.get_metadata().term;
        let data = snapshot.get_data().to_vec();
        let id = self.node.raft.id;
        let joined_as_learner = snapshot.get_metadata().get_conf_state().learners.contains(&id);
        if self.joining && !joined_as_learner {
            return Err(PartitionError::Invariant(format!(
                "this empty data directory joined as raft id {id}, but the group's snapshot at index \
                 {index} lists {id} as a voter, not a new learner: a lost voter's id cannot be \
                 recreated with empty state; stop this replica and replace the voter with a new id"
            )));
        }
        if !self.storage.apply_snapshot(snapshot)? {
            return Ok(());
        }
        self.restore_state(&data)?;
        if self.joining {
            self.storage.finish_join()?;
            self.joining = false;
        }
        self.conf_version += 1;
        self.applied = self.applied.max(index);
        let covered: Vec<(u64, u64)> = self.commit_watches
            .range(..=(index, u64::MAX))
            .map(|(&key, _)| key)
            .collect();
        for (watched, expected) in covered {
            self.commit_watches.remove(&(watched, expected));
            let result = if watched != index {
                CommitResolution::Unknown
            } else if expected == term {
                CommitResolution::Committed
            } else {
                CommitResolution::Superseded(term)
            };
            self.resolved_commits.push((watched, expected, result));
        }
        self.release_reservations(index);
        Ok(())
    }

    fn maybe_snapshot(&mut self) -> Result<(), PartitionError> {
        let snap = self.storage.snapshot_index();
        let forced = self.force_snapshot && self.applied > snap;
        self.force_snapshot = false;
        if !forced && self.applied < snap + self.snapshot_interval {
            return Ok(());
        }
        let data = self.dump_state()?;
        self.storage.create_snapshot(self.applied, data)?;
        Ok(())
    }

    fn apply(&mut self, entries: Vec<Entry>) -> Result<(), PartitionError> {
        for e in entries {
            #[cfg(test)]
            if let Some(remaining) = self.fail_apply_after.as_mut() {
                if *remaining == 0 {
                    self.fail_apply_after = None;
                    return Err(PartitionError::Engine(EngineError::Poisoned));
                }
                *remaining -= 1;
            }
            if e.get_entry_type() != EntryType::EntryNormal {
                self.apply_conf_entry(&e)?;
                self.applied = self.applied.max(e.get_index());
                self.resolve_applied(e.get_index(), e.get_term());
                self.release_reservations(e.get_index());
                continue;
            }
            if e.get_data().is_empty() {
                self.applied = self.applied.max(e.get_index());
                self.resolve_applied(e.get_index(), e.get_term());
                self.release_reservations(e.get_index());
                continue;
            }
            if e.get_entry_type() == EntryType::EntryNormal {
                let Some(cmd) = Command::decode(e.get_data()) else {
                    return Err(PartitionError::Storage(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "malformed committed command at index {} ({} bytes)",
                            e.get_index(),
                            e.get_data().len()
                        ),
                    )));
                };
                self.apply_command(cmd)?;
            }
            self.applied = self.applied.max(e.get_index());
            self.resolve_applied(e.get_index(), e.get_term());
            self.release_reservations(e.get_index());
        }
        Ok(())
    }

    fn apply_conf_entry(&mut self, e: &Entry) -> Result<(), PartitionError> {
        use protobuf::Message as _;
        let malformed = |what: &str| {
            PartitionError::Storage(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("malformed committed {what} at index {}", e.get_index()),
            ))
        };
        let changed = if e.get_entry_type() == EntryType::EntryConfChange {
            let mut cc = ConfChange::default();
            cc.merge_from_bytes(e.get_data()).map_err(|_| malformed("conf change"))?;
            self.node.apply_conf_change(&cc)
        } else {
            let mut cc = ConfChangeV2::default();
            cc.merge_from_bytes(e.get_data()).map_err(|_| malformed("conf change v2"))?;
            self.node.apply_conf_change(&cc)
        };
        let cs = match changed {
            Ok(cs) => cs,
            Err(error) => {
                log::error!(
                    "raft {}: rejected the committed membership change at index {}: {error}; \
                     configuration unchanged",
                    self.node.raft.id, e.get_index()
                );
                return Ok(());
            }
        };
        if !e.get_context().is_empty() {
            let cmd = Command::decode(e.get_context()).ok_or_else(|| malformed("conf change context"))?;
            self.apply_command(cmd)?;
        }
        let (mut voters, mut learners) = (cs.voters.clone(), cs.learners.clone());
        voters.sort_unstable();
        learners.sort_unstable();
        log::info!(
            "raft {}: applied membership change at index {}: voters {voters:?} learners {learners:?}",
            self.node.raft.id, e.get_index()
        );
        self.storage.set_conf_state(cs);
        self.conf_version += 1;
        self.force_snapshot = true;
        Ok(())
    }

    fn resolve_applied(&mut self, index: u64, term: u64) {
        let watched: Vec<u64> = self.commit_watches
            .range((index, 0)..=(index, u64::MAX))
            .map(|(&(_, expected), _)| expected)
            .collect();
        for expected in watched {
            self.commit_watches.remove(&(index, expected));
            let result = if expected == term {
                CommitResolution::Committed
            } else {
                CommitResolution::Superseded(term)
            };
            self.resolved_commits.push((index, expected, result));
        }
    }

    fn release_reservations(&mut self, through: u64) {
        let retained = self.reservations.split_off(&(through.saturating_add(1)));
        let released: usize = self.reservations.values().copied().sum();
        self.reservations = retained;
        self.reserved = self.reserved.saturating_sub(released);
    }

    fn checks_match(
        &self,
        checks: &[(Vec<u8>, Option<Vec<u8>>)],
    ) -> Result<bool, PartitionError> {
        for (key, expected) in checks {
            if self.state_get(key)? != *expected {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn apply_writes(
        &mut self,
        writes: &[(Vec<u8>, Option<Vec<u8>>)],
    ) -> Result<(), PartitionError> {
        self.note_state_writes(writes);
        match (&mut self.engine, &mut self.bounded) {
            (Some(engine), None) => {
                for (key, value) in writes {
                    match value {
                        Some(value) => engine.put(key, value, 0)?,
                        None => {
                            engine.delete(key, 0)?;
                        }
                    }
                }
                Ok(())
            }
            (None, Some(state)) => state.apply(writes),
            _ => unreachable!(),
        }
    }

    fn apply_command(&mut self, command: Command) -> Result<(), PartitionError> {
        match command {
            Command::Put(key, value) => self.apply_writes(&[(key, Some(value))]),
            Command::Delete(key) => self.apply_writes(&[(key, None)]),
            Command::ConditionalWrite { checks, key, value } => {
                if self.checks_match(&checks)? {
                    self.apply_writes(&[(key, value)])?;
                }
                Ok(())
            }
            Command::ConditionalBatch { checks, writes } => {
                if self.checks_match(&checks)? {
                    self.apply_writes(&writes)?;
                }
                Ok(())
            }
        }
    }

    pub fn id(&self) -> u64 {
        self.assert_healthy();
        self.node.raft.id
    }

    pub fn is_leader(&self) -> bool {
        self.role() == StateRole::Leader
    }

    pub fn role(&self) -> StateRole {
        self.assert_healthy();
        self.node.raft.state
    }

    pub fn role_name(&self) -> &'static str {
        match self.role() {
            StateRole::Leader => "leader",
            StateRole::Follower => "follower",
            StateRole::Candidate => "candidate",
            StateRole::PreCandidate => "pre-candidate",
        }
    }

    pub fn term(&self) -> u64 {
        self.assert_healthy();
        self.node.raft.term
    }

    pub fn leader_id(&self) -> Option<u64> {
        self.assert_healthy();
        (self.node.raft.leader_id != 0).then_some(self.node.raft.leader_id)
    }

    pub fn commit_index(&self) -> u64 {
        self.assert_healthy();
        self.node.raft.raft_log.committed
    }

    pub fn entry_term(&self, index: u64) -> Option<u64> {
        self.assert_healthy();
        let log = &self.node.raft.raft_log;
        if index < log.first_index() || index > log.last_index() {
            return None;
        }
        log.term(index).ok()
    }

    pub fn is_caught_up_leader(&self) -> bool {
        self.assert_healthy();
        if self.node.raft.state != StateRole::Leader {
            return false;
        }
        let last = self.node.raft.raft_log.last_index();
        self.applied == last
            && self.node.raft.raft_log.committed == last
            && self.node.raft.raft_log.term(last).ok() == Some(self.node.raft.term)
    }

    pub fn applied_index(&self) -> u64 {
        self.assert_healthy();
        self.applied
    }

    pub fn last_index(&self) -> u64 {
        self.assert_healthy();
        self.node.raft.raft_log.last_index()
    }

    pub fn engine(&self) -> &Engine<D> {
        self.assert_healthy();
        self.engine.as_ref().expect("bounded PD state has no storage engine")
    }
}

impl Partition<vals_spdk::MemBlockDevice> {
    pub fn new_single_bounded(id: u64, capacity: usize) -> Result<Self, PartitionError> {
        let mut partition = Self::new_bounded(id, vec![id], capacity)?;
        partition.node.campaign()?;
        Ok(partition)
    }

    pub fn new_bounded(
        id: u64,
        voters: Vec<u64>,
        capacity: usize,
    ) -> Result<Self, PartitionError> {
        Self::with_state(
            id,
            PdStorage::in_memory(voters),
            None,
            Some(BoundedState::new(capacity)),
        )
    }

    pub fn open_single_bounded(
        id: u64,
        capacity: usize,
        dir: impl AsRef<Path>,
    ) -> Result<Self, PartitionError> {
        let mut partition = Self::open_bounded(id, vec![id], capacity, dir)?;
        partition.drive()?;
        partition.campaign()?;
        Ok(partition)
    }

    pub fn open_bounded(
        id: u64,
        voters: Vec<u64>,
        capacity: usize,
        dir: impl AsRef<Path>,
    ) -> Result<Self, PartitionError> {
        Self::open_bounded_storage(id, PdStorage::open(dir, voters)?, capacity)
    }

    pub fn open_bounded_joining(
        id: u64,
        capacity: usize,
        dir: impl AsRef<Path>,
    ) -> Result<Self, PartitionError> {
        Self::open_bounded_storage(id, PdStorage::open_joining(dir)?, capacity)
    }

    fn open_bounded_storage(
        id: u64,
        storage: PdStorage,
        capacity: usize,
    ) -> Result<Self, PartitionError> {
        let mut state = BoundedState::new(capacity);
        state.restore(&storage.snapshot_data())?;
        Self::with_state(id, storage, None, Some(state))
    }
}

pub fn seed_snapshot(
    dir: impl AsRef<Path>,
    voters: Vec<u64>,
    index: u64,
    term: u64,
    state: &[(Vec<u8>, Vec<u8>)],
) -> Result<(), PartitionError> {
    let dir = dir.as_ref();
    if index == 0 || term == 0 {
        return Err(PartitionError::Invariant("a seeded snapshot needs a non-zero index and term".into()));
    }
    if voters.is_empty() {
        return Err(PartitionError::Invariant("a seeded snapshot needs at least one voter".into()));
    }
    if dir.exists() && std::fs::read_dir(dir)?.next().is_some() {
        return Err(PartitionError::Invariant(format!(
            "{} is not empty; a snapshot is seeded only into fresh raft storage",
            dir.display()
        )));
    }
    let mut data = Vec::new();
    for (i, (key, value)) in state.iter().enumerate() {
        if i > 0 && state[i - 1].0 >= *key {
            return Err(PartitionError::Invariant("seeded state keys must be strictly increasing".into()));
        }
        put_field(&mut data, key);
        put_field(&mut data, value);
    }
    let parent = match dir.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    let name = dir.file_name().ok_or_else(|| PartitionError::Invariant(format!("{} has no name", dir.display())))?;
    std::fs::create_dir_all(parent)?;
    let staging = parent.join(format!(".{}.seeding", name.to_string_lossy()));
    if staging.exists() {
        std::fs::remove_dir_all(&staging)?;
    }
    let storage = PdStorage::open(&staging, voters.clone())?;
    let mut snapshot = Snapshot::default();
    snapshot.set_data(data.into());
    let meta = snapshot.mut_metadata();
    meta.set_index(index);
    meta.set_term(term);
    meta.set_conf_state(ConfState::from((voters, vec![])));
    storage.apply_snapshot(snapshot)?;
    drop(storage);
    std::fs::rename(&staging, dir)?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use vals_spdk::MemBlockDevice;
    use vals_store::{Config as StoreConfig, Ordering};

    fn engine() -> Engine<MemBlockDevice> {
        let dev = MemBlockDevice::new(256, 512);
        Engine::create_prewiped(
            dev,
            StoreConfig {
                seg_blocks: 4,
                reserved_blocks: 0,
                ordering: Ordering::Unsorted,
            },
        )
    }

    fn settle<D: BlockDevice>(p: &mut Partition<D>) {
        for _ in 0..50 {
            p.tick();
            p.drive().unwrap();
        }
    }

    #[test]
    fn malformed_committed_command_fails_apply_loudly() {
        let mut p = Partition::new_single(1, engine()).unwrap();
        settle(&mut p);
        let before = p.applied_index();
        let mut e = Entry::default();
        e.set_entry_type(EntryType::EntryNormal);
        e.set_index(before + 1);
        e.set_term(1);
        e.set_data(vec![0xFF, 1, 2, 3].into());
        let err = p.apply(vec![e]).unwrap_err();
        assert!(
            matches!(err, PartitionError::Storage(_)),
            "expected a loud Storage error, got {err:?}"
        );
        assert_eq!(
            p.applied_index(),
            before,
            "a malformed entry must not advance the applied index"
        );
    }

    #[test]
    fn commit_watches_distinguish_terms_at_same_index() {
        let mut p = Partition::new_single(1, engine()).unwrap();
        settle(&mut p);
        let index = p.propose_index(&Command::Put(b"key".to_vec(), b"value".to_vec()))
            .unwrap();
        let term = p.entry_term(index).unwrap();
        p.watch_commit(index, term);
        p.watch_commit(index, term + 1);
        settle(&mut p);
        let resolved = p.take_resolved_commits();
        assert!(resolved.contains(&(index, term, CommitResolution::Committed)));
        assert!(resolved.contains(&(index, term + 1, CommitResolution::Superseded(term))));
    }

    #[test]
    fn command_roundtrips() {
        for cmd in [
            Command::Put(b"key".to_vec(), b"value".to_vec()),
            Command::Delete(b"key".to_vec()),
            Command::Put(vec![], vec![]),
            Command::ConditionalWrite {
                checks: vec![(b"key".to_vec(), Some(b"old".to_vec())), (b"free".to_vec(), None)],
                key: b"key".to_vec(),
                value: Some(b"new".to_vec()),
            },
            Command::ConditionalBatch {
                checks: vec![(b"generation".to_vec(), Some(7u64.to_le_bytes().to_vec()))],
                writes: vec![
                    (b"generation".to_vec(), Some(8u64.to_le_bytes().to_vec())),
                    (b"old".to_vec(), None),
                ],
            },
        ] {
            assert_eq!(Command::decode(&cmd.encode()), Some(cmd));
        }
    }

    #[test]
    fn bounded_state_reserves_inflight_growth_and_rejects_before_exhaustion() {
        let mut partition = Partition::new_single_bounded(1, 60).unwrap();
        settle(&mut partition);
        partition.propose(&Command::Put(b"key".to_vec(), vec![1; 10])).unwrap();
        assert_eq!(partition.state_reserved(), 45);
        let error = partition
            .propose(&Command::Put(b"other".to_vec(), vec![2; 10]))
            .unwrap_err();
        assert!(matches!(error, PartitionError::CapacityExceeded { .. }));
        settle(&mut partition);
        assert_eq!(partition.state_usage(), 45);
        assert_eq!(partition.state_reserved(), 0);
        assert_eq!(partition.state_get(b"key").unwrap(), Some(vec![1; 10]));
        partition.propose(&Command::ConditionalBatch {
            checks: Vec::new(),
            writes: vec![
                (b"key".to_vec(), None),
                (b"new".to_vec(), Some(vec![2; 10])),
            ],
        }).unwrap();
        assert_eq!(partition.state_reserved(), 0, "redistribution has zero net growth");
        let error = partition
            .propose(&Command::Put(b"key".to_vec(), vec![3; 10]))
            .unwrap_err();
        assert!(matches!(error, PartitionError::CapacityExceeded { .. }));
        settle(&mut partition);
        assert_eq!(partition.state_usage(), 45);
        assert_eq!(partition.state_get(b"key").unwrap(), None);
        assert_eq!(partition.state_get(b"new").unwrap(), Some(vec![2; 10]));
    }

    fn settle_cluster(nodes: &mut [Partition<MemBlockDevice>]) {
        for _ in 0..100 {
            let mut msgs = Vec::new();
            for n in nodes.iter_mut() {
                n.drive().unwrap();
                msgs.append(&mut n.take_outgoing());
            }
            for m in msgs {
                let to = m.to;
                if let Some(n) = nodes.iter_mut().find(|n| n.id() == to) {
                    n.step(m).unwrap();
                }
            }
        }
    }

    #[test]
    fn seeded_snapshot_starts_every_voter_at_the_same_state() {
        let root = tmp_dir("seeded");
        let state = vec![(b"a".to_vec(), b"1".to_vec()), (b"b".to_vec(), b"2".to_vec())];
        for id in 1..=3u64 {
            seed_snapshot(root.join(id.to_string()), vec![1, 2, 3], 40, 7, &state).unwrap();
        }
        assert!(matches!(
            seed_snapshot(root.join("1"), vec![1, 2, 3], 40, 7, &state),
            Err(PartitionError::Invariant(e)) if e.contains("not empty")
        ));
        let unsorted = vec![state[1].clone(), state[0].clone()];
        assert!(seed_snapshot(root.join("bad"), vec![1], 40, 7, &unsorted).is_err());
        let mut nodes: Vec<_> = (1..=3u64)
            .map(|id| Partition::open_bounded(id, vec![9], 4096, root.join(id.to_string())).unwrap())
            .collect();
        for node in &nodes {
            assert_eq!(node.applied_index(), 40);
            assert_eq!(node.state_get(b"b").unwrap(), Some(b"2".to_vec()));
        }
        nodes[1].campaign().unwrap();
        settle_cluster(&mut nodes);
        assert!(nodes[1].is_leader());
        assert!(nodes[1].term() > 7);
        nodes[1].propose(&Command::Put(b"c".to_vec(), b"3".to_vec())).unwrap();
        settle_cluster(&mut nodes);
        for node in &nodes {
            assert_eq!(node.state_get(b"c").unwrap(), Some(b"3".to_vec()));
            assert_eq!(node.state_get(b"a").unwrap(), Some(b"1".to_vec()));
            assert!(node.applied_index() > 40);
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn bounded_followers_apply_conditional_batches_deterministically() {
        let mut nodes = vec![
            Partition::new_bounded(1, vec![1, 2, 3], 1024).unwrap(),
            Partition::new_bounded(2, vec![1, 2, 3], 1024).unwrap(),
            Partition::new_bounded(3, vec![1, 2, 3], 1024).unwrap(),
        ];
        nodes[0].campaign().unwrap();
        settle_cluster(&mut nodes);
        nodes[0].propose(&Command::ConditionalBatch {
            checks: vec![(b"generation".to_vec(), None)],
            writes: vec![
                (b"generation".to_vec(), Some(1u64.to_le_bytes().to_vec())),
                (b"range".to_vec(), Some(b"record".to_vec())),
            ],
        }).unwrap();
        settle_cluster(&mut nodes);
        for node in &nodes {
            assert_eq!(node.state_get(b"generation").unwrap(), Some(1u64.to_le_bytes().to_vec()));
            assert_eq!(node.state_get(b"range").unwrap(), Some(b"record".to_vec()));
            assert_eq!(node.state_usage(), 93);
        }
    }

    fn settle_cluster_wire(nodes: &mut [Partition<MemBlockDevice>]) {
        for _ in 0..100 {
            let mut msgs = Vec::new();
            for n in nodes.iter_mut() {
                n.drive().unwrap();
                for m in n.take_outgoing() {
                    msgs.push(decode_message(&encode_message(&m)).expect("codec round-trip"));
                }
            }
            for m in msgs {
                let to = m.to;
                if let Some(n) = nodes.iter_mut().find(|n| n.id() == to) {
                    n.step(m).unwrap();
                }
            }
        }
    }

    fn tcp_roundtrip(msg: &Message) -> Message {
        use std::io::{Read, Write};
        use std::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(addr).unwrap();
        let (mut server, _) = listener.accept().unwrap();

        let payload = encode_message(msg);
        client
            .write_all(&(payload.len() as u32).to_le_bytes())
            .unwrap();
        client.write_all(&payload).unwrap();

        let mut len = [0u8; 4];
        server.read_exact(&mut len).unwrap();
        let mut buf = vec![0u8; u32::from_le_bytes(len) as usize];
        server.read_exact(&mut buf).unwrap();
        decode_message(&buf).unwrap()
    }

    fn settle_cluster_tcp(nodes: &mut [Partition<MemBlockDevice>]) {
        for _ in 0..100 {
            let mut msgs = Vec::new();
            for n in nodes.iter_mut() {
                n.drive().unwrap();
                for m in n.take_outgoing() {
                    msgs.push(tcp_roundtrip(&m));
                }
            }
            for m in msgs {
                let to = m.to;
                if let Some(n) = nodes.iter_mut().find(|n| n.id() == to) {
                    n.step(m).unwrap();
                }
            }
        }
    }

    #[test]
    fn consensus_over_real_tcp() {
        let mut nodes = vec![
            Partition::new(1, vec![1, 2, 3], engine()).unwrap(),
            Partition::new(2, vec![1, 2, 3], engine()).unwrap(),
            Partition::new(3, vec![1, 2, 3], engine()).unwrap(),
        ];
        nodes[0].campaign().unwrap();
        settle_cluster_tcp(&mut nodes);
        assert!(nodes[0].is_leader(), "elected over TCP");
        nodes[0]
            .propose(&Command::Put(b"net".to_vec(), b"ok".to_vec()))
            .unwrap();
        settle_cluster_tcp(&mut nodes);
        for n in &nodes {
            assert_eq!(n.engine().get(b"net").unwrap().as_deref(), Some(&b"ok"[..]));
        }
    }

    #[test]
    fn conditional_write_applies_identically_on_a_higher_term_lagging_replica() {
        let mut current = Partition::new_single(1, engine()).unwrap();
        let mut lagging = Partition::new_single(2, engine()).unwrap();
        current.drive().unwrap();
        lagging.drive().unwrap();
        lagging.node.raft.term = current.node.raft.term.saturating_add(7);
        let mut entry = Entry::default();
        entry.set_index(current.applied_index().max(lagging.applied_index()) + 1);
        entry.set_term(current.node.raft.term);
        entry.set_entry_type(EntryType::EntryNormal);
        entry.set_data(Command::ConditionalWrite {
            checks: vec![(b"lock".to_vec(), None)],
            key: b"lock".to_vec(),
            value: Some(b"intent".to_vec()),
        }.encode().into());
        current.apply(vec![entry.clone()]).unwrap();
        lagging.apply(vec![entry]).unwrap();
        assert_eq!(current.engine().get(b"lock").unwrap(), lagging.engine().get(b"lock").unwrap());
        assert_eq!(current.engine().get(b"lock").unwrap().as_deref(), Some(&b"intent"[..]));
    }

    #[test]
    fn conditional_single_key_phases_are_replay_safe() {
        let mut partition = Partition::new_single(1, engine()).unwrap();
        partition.drive().unwrap();
        let apply = |partition: &mut Partition<MemBlockDevice>, command: Command| {
            let mut entry = Entry::default();
            entry.set_index(partition.applied_index() + 1);
            entry.set_term(1);
            entry.set_entry_type(EntryType::EntryNormal);
            entry.set_data(command.encode().into());
            partition.apply(vec![entry]).unwrap();
        };
        let lock = b"lock".to_vec();
        let map = b"map".to_vec();
        let intent = Command::ConditionalWrite {
            checks: vec![(lock.clone(), None)],
            key: lock.clone(),
            value: Some(b"intent".to_vec()),
        };
        apply(&mut partition, intent.clone());
        apply(&mut partition, intent);
        let prepare = Command::ConditionalWrite {
            checks: vec![(lock.clone(), Some(b"intent".to_vec())), (map.clone(), None)],
            key: map.clone(),
            value: Some(b"pending".to_vec()),
        };
        apply(&mut partition, prepare.clone());
        apply(&mut partition, prepare);
        let clear = Command::ConditionalWrite {
            checks: vec![(lock.clone(), Some(b"intent".to_vec()))],
            key: lock.clone(),
            value: None,
        };
        apply(&mut partition, clear.clone());
        apply(&mut partition, clear);
        assert!(partition.engine().get(&lock).unwrap().is_none());
        assert_eq!(partition.engine().get(&map).unwrap().as_deref(), Some(&b"pending"[..]));
    }

    #[test]
    fn conditional_single_key_phases_serialize_delayed_global_intents() {
        let mut nodes = vec![
            Partition::new(1, vec![1, 2, 3], engine()).unwrap(),
            Partition::new(2, vec![1, 2, 3], engine()).unwrap(),
            Partition::new(3, vec![1, 2, 3], engine()).unwrap(),
        ];
        nodes[0].campaign().unwrap();
        settle_cluster(&mut nodes);
        let lock = b"balance-lock".to_vec();
        let intent = |range: &[u8]| Command::ConditionalWrite {
            checks: vec![(lock.clone(), None)],
            key: lock.clone(),
            value: Some(range.to_vec()),
        };
        nodes[0].propose(&intent(b"zone-7")).unwrap();
        nodes[0].propose(&intent(b"zone-8")).unwrap();
        assert!(nodes[0].engine().get(&lock).unwrap().is_none());
        settle_cluster(&mut nodes);
        for node in &nodes {
            assert_eq!(node.engine().get(&lock).unwrap().as_deref(), Some(&b"zone-7"[..]));
        }

        let zone7 = b"zone-7".to_vec();
        let zone8 = b"zone-8".to_vec();
        nodes[0].propose(&Command::ConditionalWrite {
            checks: vec![(lock.clone(), Some(zone7.clone())), (zone7.clone(), None)],
            key: zone7.clone(),
            value: Some(b"pending".to_vec()),
        }).unwrap();
        nodes[0].propose(&Command::ConditionalWrite {
            checks: vec![(lock.clone(), None), (zone8.clone(), None)],
            key: zone8.clone(),
            value: Some(b"split".to_vec()),
        }).unwrap();
        settle_cluster(&mut nodes);
        for node in &nodes {
            assert_eq!(node.engine().get(&zone7).unwrap().as_deref(), Some(&b"pending"[..]));
            assert!(node.engine().get(&zone8).unwrap().is_none());
        }
    }

    #[test]
    fn consensus_survives_message_serialization() {
        let mut nodes = vec![
            Partition::new(1, vec![1, 2, 3], engine()).unwrap(),
            Partition::new(2, vec![1, 2, 3], engine()).unwrap(),
            Partition::new(3, vec![1, 2, 3], engine()).unwrap(),
        ];
        nodes[0].campaign().unwrap();
        settle_cluster_wire(&mut nodes);
        assert!(nodes[0].is_leader());
        nodes[0]
            .propose(&Command::Put(b"k".to_vec(), b"v".to_vec()))
            .unwrap();
        settle_cluster_wire(&mut nodes);
        for n in &nodes {
            assert_eq!(n.engine().get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
        }
    }

    #[test]
    fn three_node_replicates_via_consensus() {
        let mut nodes = vec![
            Partition::new(1, vec![1, 2, 3], engine()).unwrap(),
            Partition::new(2, vec![1, 2, 3], engine()).unwrap(),
            Partition::new(3, vec![1, 2, 3], engine()).unwrap(),
        ];
        nodes[0].campaign().unwrap();
        settle_cluster(&mut nodes);
        assert!(nodes[0].is_leader(), "campaigned node leads");
        assert_eq!(nodes.iter().filter(|n| n.is_leader()).count(), 1);

        nodes[0]
            .propose(&Command::Put(b"k".to_vec(), b"v".to_vec()))
            .unwrap();
        settle_cluster(&mut nodes);
        for n in &nodes {
            assert_eq!(n.engine().get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
        }

        nodes[1]
            .propose(&Command::Put(b"x".to_vec(), b"1".to_vec()))
            .unwrap();
        settle_cluster(&mut nodes);
        for n in &nodes {
            assert_eq!(n.engine().get(b"x").unwrap().as_deref(), Some(&b"1"[..]));
        }
    }

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("vals-raft-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn durable_partition_recovers_state_across_restart() {
        let dir = tmp_dir("recover");
        let (applied_before, term_before) = {
            let mut p = Partition::open(1, vec![1], engine(), &dir).unwrap();
            p.campaign().unwrap();
            settle(&mut p);
            assert!(p.is_leader());
            p.propose(&Command::Put(b"a".to_vec(), b"1".to_vec()))
                .unwrap();
            p.propose(&Command::Put(b"b".to_vec(), b"2".to_vec()))
                .unwrap();
            p.propose(&Command::Delete(b"a".to_vec())).unwrap();
            settle(&mut p);
            assert_eq!(p.engine().get(b"a").unwrap(), None);
            assert_eq!(p.engine().get(b"b").unwrap().as_deref(), Some(&b"2"[..]));
            let key = b"intent".to_vec();
            p.propose(&Command::ConditionalWrite {
                checks: vec![(key.clone(), None)],
                key,
                value: Some(b"phase-1".to_vec()),
            }).unwrap();
            settle(&mut p);
            assert_eq!(p.engine().get(b"intent").unwrap().as_deref(), Some(&b"phase-1"[..]));
            (p.applied_index(), p.storage.hard_state().term)
        };
        assert!(applied_before > 0);

        let mut p = Partition::open(1, vec![1], engine(), &dir).unwrap();
        p.campaign().unwrap();
        settle(&mut p);
        assert_eq!(p.engine().get(b"a").unwrap(), None);
        assert_eq!(p.engine().get(b"b").unwrap().as_deref(), Some(&b"2"[..]));
        assert_eq!(p.engine().get(b"intent").unwrap().as_deref(), Some(&b"phase-1"[..]));
        assert!(
            p.applied_index() >= applied_before,
            "applied index (config epoch) must not regress: {} < {applied_before}",
            p.applied_index()
        );
        assert!(
            p.storage.hard_state().term > term_before,
            "recovered term must carry forward (no double-vote in an old term)"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn durable_partition_compacts_and_recovers_via_snapshot() {
        let dir = tmp_dir("compact");
        let applied_before = {
            let mut p = Partition::open(1, vec![1], engine(), &dir)
                .unwrap()
                .with_snapshot_interval(8);
            p.campaign().unwrap();
            settle(&mut p);
            for i in 0..20u8 {
                p.propose(&Command::Put(vec![b'k', i], vec![i])).unwrap();
                p.tick();
                p.drive().unwrap();
            }
            settle(&mut p);
            assert!(p.snapshot_index() > 0, "log compacted behind a snapshot");
            p.applied_index()
        };

        let mut p = Partition::open(1, vec![1], engine(), &dir)
            .unwrap()
            .with_snapshot_interval(8);
        assert!(!p.engine().is_empty(), "snapshot restored into the engine");
        p.campaign().unwrap();
        settle(&mut p);
        for i in 0..20u8 {
            assert_eq!(
                p.engine().get(&[b'k', i]).unwrap().as_deref(),
                Some(&[i][..]),
                "key k{i} survives compaction + restart"
            );
        }
        assert!(p.applied_index() >= applied_before);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn bounded_state_recovers_from_durable_snapshot_and_log() {
        let dir = tmp_dir("bounded-compact");
        let applied = {
            let mut partition = Partition::open_bounded(1, vec![1], 4096, &dir)
                .unwrap()
                .with_snapshot_interval(4);
            partition.campaign().unwrap();
            settle(&mut partition);
            for i in 0..12u8 {
                partition.propose(&Command::Put(vec![b'k', i], vec![i; 8])).unwrap();
                settle(&mut partition);
            }
            assert!(partition.snapshot_index() > 0);
            partition.applied_index()
        };
        let mut partition = Partition::open_bounded(1, vec![1], 4096, &dir)
            .unwrap()
            .with_snapshot_interval(4);
        assert!(partition.state_usage() > 0, "snapshot restored live state");
        partition.campaign().unwrap();
        settle(&mut partition);
        assert_eq!(partition.state_usage(), 504);
        assert!(partition.applied_index() >= applied);
        for i in 0..12u8 {
            assert_eq!(partition.state_get(&[b'k', i]).unwrap(), Some(vec![i; 8]));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn lagging_follower_catches_up_via_snapshot() {
        let dir = tmp_dir("follower-snap");
        let mut nodes = vec![
            Partition::new(1, vec![1, 2, 3], engine())
                .unwrap()
                .with_snapshot_interval(4),
            Partition::new(2, vec![1, 2, 3], engine())
                .unwrap()
                .with_snapshot_interval(4),
            Partition::open(3, vec![1, 2, 3], engine(), &dir)
                .unwrap()
                .with_snapshot_interval(4),
        ];
        nodes[0].campaign().unwrap();
        settle_cluster(&mut nodes);
        assert!(nodes[0].is_leader());

        for i in 0..16u8 {
            nodes[0]
                .propose(&Command::Put(vec![b'k', i], vec![i]))
                .unwrap();
            for _ in 0..20 {
                let mut msgs = Vec::new();
                for n in nodes[..2].iter_mut() {
                    n.drive().unwrap();
                    msgs.append(&mut n.take_outgoing());
                }
                for m in msgs {
                    if m.to == 3 {
                        continue;
                    }
                    if let Some(n) = nodes[..2].iter_mut().find(|n| n.id() == m.to) {
                        n.step(m).unwrap();
                    }
                }
            }
        }
        assert!(
            nodes[0].snapshot_index() > 0,
            "leader compacted its log while node 3 was away"
        );
        assert_eq!(nodes[2].engine().len(), 0, "node 3 saw none of it");

        for _ in 0..100 {
            let mut msgs = Vec::new();
            for n in nodes.iter_mut() {
                n.tick();
                n.drive().unwrap();
                msgs.append(&mut n.take_outgoing());
            }
            for m in msgs {
                let to = m.to;
                if let Some(n) = nodes.iter_mut().find(|n| n.id() == to) {
                    n.step(m).unwrap();
                }
            }
        }
        for i in 0..16u8 {
            assert_eq!(
                nodes[2].engine().get(&[b'k', i]).unwrap().as_deref(),
                Some(&[i][..]),
                "follower caught up to key k{i} via snapshot"
            );
        }
        assert!(nodes[2].snapshot_index() > 0, "snapshot installed");
        let applied = nodes[2].applied_index();

        drop(nodes);
        let p = Partition::open(3, vec![1, 2, 3], engine(), &dir).unwrap();
        assert!(p.applied_index() >= 4, "snapshot survives restart");
        for i in 0..16u8 {
            if (i as u64) < applied {
                let _ = p.engine().get(&[b'k', i]).unwrap();
            }
        }
        assert!(!p.engine().is_empty(), "engine restored from disk snapshot");
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn assert_terminal<D: BlockDevice>(p: &mut Partition<D>, error: PartitionError) {
        assert!(
            matches!(error, PartitionError::Storage(_) | PartitionError::Engine(_)),
            "unexpected drive error: {error:?}"
        );
        assert!(p.take_outgoing().is_empty(), "failed Ready leaked messages");
        assert!(matches!(p.drive(), Err(PartitionError::RestartRequired)));
        assert!(matches!(
            p.propose(&Command::Put(b"after".to_vec(), b"failure".to_vec())),
            Err(PartitionError::RestartRequired)
        ));
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = p.engine().len();
        })).is_err(), "failed state machine remained readable");
    }

    #[test]
    fn append_hard_state_and_light_ready_commit_failures_are_terminal() {
        for op in [
            storage::StorageOp::Append,
            storage::StorageOp::HardState,
            storage::StorageOp::Commit,
        ] {
            let mut p = Partition::new_single(1, engine()).unwrap();
            p.storage.fail_next(op);
            let error = p.drive().expect_err("injected persistence failure was reached");
            assert_terminal(&mut p, error);
        }
    }

    #[test]
    fn incoming_snapshot_failure_is_terminal_and_restart_repairs_partial_persistence() {
        let dir = tmp_dir("incoming-snapshot-failure");
        let mut nodes = vec![
            Partition::new(1, vec![1, 2, 3], engine())
                .unwrap()
                .with_snapshot_interval(4),
            Partition::new(2, vec![1, 2, 3], engine())
                .unwrap()
                .with_snapshot_interval(4),
            Partition::open(3, vec![1, 2, 3], engine(), &dir)
                .unwrap()
                .with_snapshot_interval(4),
        ];
        nodes[0].campaign().unwrap();
        settle_cluster(&mut nodes);
        for i in 0..12u8 {
            nodes[0]
                .propose(&Command::Put(vec![b's', i], vec![i]))
                .unwrap();
            for _ in 0..10 {
                let mut messages = Vec::new();
                for node in &mut nodes[..2] {
                    node.drive().unwrap();
                    messages.append(&mut node.take_outgoing());
                }
                for message in messages {
                    if message.to == 3 {
                        continue;
                    }
                    let target = (message.to - 1) as usize;
                    nodes[target].step(message).unwrap();
                }
            }
        }
        assert!(nodes[0].snapshot_index() > nodes[2].last_index());
        nodes[2]
            .storage
            .fail_next(storage::StorageOp::ApplySnapshot);

        let mut failure = None;
        'catch_up: for _ in 0..200 {
            let mut messages = Vec::new();
            for (index, node) in nodes.iter_mut().enumerate() {
                node.tick();
                match node.drive() {
                    Ok(()) => messages.append(&mut node.take_outgoing()),
                    Err(error) if index == 2 => {
                        failure = Some(error);
                        break 'catch_up;
                    }
                    Err(error) => panic!("healthy peer failed: {error}"),
                }
            }
            for message in messages {
                let target = (message.to - 1) as usize;
                nodes[target].step(message).unwrap();
            }
        }
        let error = failure.expect("lagging peer received the compacted snapshot");
        assert_terminal(&mut nodes[2], error);
        drop(nodes);

        let recovered = Partition::open(3, vec![1, 2, 3], engine(), &dir).unwrap();
        assert!(recovered.snapshot_index() > 0);
        for i in 0..11u8 {
            assert_eq!(
                recovered.engine().get(&[b's', i]).unwrap().as_deref(),
                Some(&[i][..])
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn persistence_failure_discards_all_unsent_outbox_traffic() {
        let mut follower = Partition::new(2, vec![1, 2], engine()).unwrap();
        let mut heartbeat = Message::default();
        heartbeat.set_msg_type(MessageType::MsgHeartbeat);
        heartbeat.set_from(1);
        heartbeat.set_to(2);
        heartbeat.set_term(1);
        follower.step(heartbeat.clone()).unwrap();
        follower.drive().unwrap();
        follower.take_outgoing();

        follower.step(heartbeat).unwrap();
        for _ in 0..10 {
            follower.tick();
        }
        let mut vote = Message::default();
        vote.set_msg_type(MessageType::MsgRequestVote);
        vote.set_from(1);
        vote.set_to(2);
        vote.set_term(2);
        follower.step(vote).unwrap();
        follower.storage.fail_next(storage::StorageOp::HardState);
        let error = follower.drive().expect_err("vote hard state failure was injected");
        assert_terminal(&mut follower, error);
    }

    #[test]
    fn local_snapshot_failure_is_terminal_and_restart_repairs_partial_persistence() {
        let dir = tmp_dir("snapshot-failure");
        let mut p = Partition::open(1, vec![1], engine(), &dir)
            .unwrap()
            .with_snapshot_interval(1);
        p.campaign().unwrap();
        p.storage.fail_next(storage::StorageOp::CreateSnapshot);
        let error = p.drive().expect_err("local snapshot persistence must be attempted");
        assert_terminal(&mut p, error);
        drop(p);

        let mut recovered = Partition::open(1, vec![1], engine(), &dir).unwrap();
        recovered.drive().unwrap();
        assert!(recovered.applied_index() >= recovered.snapshot_index());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn light_ready_mid_batch_apply_failure_recovers_all_committed_commands() {
        let dir = tmp_dir("light-apply-failure");
        let mut p = Partition::open(1, vec![1], engine(), &dir).unwrap();
        p.campaign().unwrap();
        settle(&mut p);
        for i in 0..3u8 {
            p.propose(&Command::Put(vec![b'k', i], vec![i])).unwrap();
        }
        p.fail_apply_after = Some(1);
        let error = p.drive().expect_err("LightReady apply failure was injected");
        assert_eq!(p.engine.as_ref().unwrap().get(&[b'k', 0]).unwrap().as_deref(), Some(&[0][..]));
        assert_eq!(p.engine.as_ref().unwrap().get(&[b'k', 1]).unwrap(), None);
        assert_terminal(&mut p, error);
        drop(p);

        let mut recovered = Partition::open(1, vec![1], engine(), &dir).unwrap();
        recovered.drive().unwrap();
        for i in 0..3u8 {
            assert_eq!(
                recovered.engine().get(&[b'k', i]).unwrap().as_deref(),
                Some(&[i][..])
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ready_mid_batch_apply_failure_recovers_all_committed_commands() {
        let dir = tmp_dir("ready-apply-failure");
        {
            let mut p = Partition::open(1, vec![1], engine(), &dir).unwrap();
            p.campaign().unwrap();
            settle(&mut p);
            for i in 0..3u8 {
                p.propose(&Command::Put(vec![b'r', i], vec![i])).unwrap();
            }
            settle(&mut p);
        }

        let mut failed = Partition::open(1, vec![1], engine(), &dir).unwrap();
        failed.fail_apply_after = Some(2);
        let error = failed.drive().expect_err("Ready apply failure was injected");
        assert_eq!(failed.engine.as_ref().unwrap().get(&[b'r', 0]).unwrap().as_deref(), Some(&[0][..]));
        assert_eq!(failed.engine.as_ref().unwrap().get(&[b'r', 1]).unwrap(), None);
        assert_terminal(&mut failed, error);
        drop(failed);

        let mut recovered = Partition::open(1, vec![1], engine(), &dir).unwrap();
        recovered.drive().unwrap();
        for i in 0..3u8 {
            assert_eq!(
                recovered.engine().get(&[b'r', i]).unwrap().as_deref(),
                Some(&[i][..])
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn healthy_pre_persistence_messages_are_still_delivered() {
        let mut follower = Partition::new(2, vec![1, 2], engine()).unwrap();
        let mut heartbeat = Message::default();
        heartbeat.set_msg_type(MessageType::MsgHeartbeat);
        heartbeat.set_from(1);
        heartbeat.set_to(2);
        heartbeat.set_term(1);
        follower.step(heartbeat.clone()).unwrap();
        follower.drive().unwrap();
        follower.take_outgoing();

        follower.step(heartbeat).unwrap();
        follower.drive().unwrap();
        assert!(follower.take_outgoing().iter().any(|message| {
            message.get_msg_type() == MessageType::MsgHeartbeatResponse
        }));
    }

    fn route(nodes: &mut [Partition<MemBlockDevice>], down: &[u64], rounds: usize, tick: bool) {
        for _ in 0..rounds {
            let mut msgs = Vec::new();
            for n in nodes.iter_mut() {
                if down.contains(&n.node.raft.id) {
                    continue;
                }
                if tick {
                    n.tick();
                }
                n.drive().unwrap();
                msgs.append(&mut n.take_outgoing());
            }
            for m in msgs {
                if down.contains(&m.to) {
                    continue;
                }
                if let Some(n) = nodes.iter_mut().find(|n| n.node.raft.id == m.to) {
                    n.step(m).unwrap();
                }
            }
        }
    }

    fn voters_and_learners(p: &Partition<MemBlockDevice>) -> (Vec<u64>, Vec<u64>) {
        let cs = p.conf_state();
        let (mut voters, mut learners) = (cs.voters, cs.learners);
        voters.sort_unstable();
        learners.sort_unstable();
        (voters, learners)
    }

    fn three_durable(root: &std::path::Path) -> Vec<Partition<MemBlockDevice>> {
        (1..=3u64)
            .map(|id| Partition::open_bounded(id, vec![1, 2, 3], 1 << 20, root.join(id.to_string())).unwrap())
            .collect()
    }

    #[test]
    fn learner_replaces_a_lost_voter_and_restores_redundancy() {
        let root = tmp_dir("replace");
        let mut nodes = three_durable(&root);
        nodes[0].campaign().unwrap();
        route(&mut nodes, &[], 50, false);
        assert!(nodes[0].is_leader());
        for i in 0..10u8 {
            nodes[0].propose(&Command::Put(vec![b'k', i], vec![i])).unwrap();
        }
        route(&mut nodes, &[], 50, false);
        assert_eq!(nodes[0].snapshot_index(), 0, "no compaction before the membership change");

        drop(nodes.pop());
        let _ = std::fs::remove_dir_all(root.join("3"));
        nodes.push(Partition::open_bounded_joining(4, 1 << 20, root.join("4")).unwrap());
        assert!(nodes[2].is_joining());
        assert_eq!(voters_and_learners(&nodes[2]), (vec![], vec![]));

        let add = nodes[0]
            .propose_conf_change(
                ConfChangeType::AddLearnerNode,
                4,
                Some(&Command::Put(b"peer-4".to_vec(), b"addr".to_vec())),
            )
            .unwrap();
        assert!(matches!(
            nodes[0].propose_conf_change(ConfChangeType::RemoveNode, 3, None),
            Err(PartitionError::NotReady)
        ), "a second membership change waits for the first to apply");
        route(&mut nodes, &[], 100, true);
        assert!(nodes[0].snapshot_index() >= add, "applying the change compacted the leader");
        assert!(!nodes[2].is_joining(), "the learner installed a snapshot");
        assert!(!root.join("4").join("joining").exists());
        for node in &nodes {
            assert_eq!(voters_and_learners(node), (vec![1, 2, 3], vec![4]));
            assert_eq!(node.state_get(b"peer-4").unwrap(), Some(b"addr".to_vec()));
            assert_eq!(node.state_get(&[b'k', 9]).unwrap(), Some(vec![9]));
        }
        assert!(nodes[0].peer_matched(4).unwrap() >= nodes[0].commit_index());

        nodes[0].propose_conf_change(ConfChangeType::RemoveNode, 3, None).unwrap();
        route(&mut nodes, &[], 50, true);
        nodes[0].propose_conf_change(ConfChangeType::AddNode, 4, None).unwrap();
        route(&mut nodes, &[], 50, true);
        for node in &nodes {
            assert_eq!(voters_and_learners(node), (vec![1, 2, 4], vec![]));
        }

        let mut survivors: Vec<_> = nodes.drain(1..).collect();
        drop(nodes);
        survivors[1].force_campaign().unwrap();
        route(&mut survivors, &[], 50, false);
        assert!(survivors[1].is_leader(), "the replacement can lead with one original voter");
        survivors[1].propose(&Command::Put(b"after".to_vec(), b"loss".to_vec())).unwrap();
        route(&mut survivors, &[], 50, false);
        assert_eq!(survivors[0].state_get(b"after").unwrap(), Some(b"loss".to_vec()));
        drop(survivors);

        let reopened = Partition::open_bounded(4, vec![9], 1 << 20, root.join("4")).unwrap();
        assert_eq!(voters_and_learners(&reopened), (vec![1, 2, 4], vec![]));
        assert_eq!(reopened.state_get(&[b'k', 0]).unwrap(), Some(vec![0]));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn membership_change_survives_a_crash_while_compacting() {
        let dir = tmp_dir("conf-crash");
        {
            let mut p = Partition::open_bounded(1, vec![1], 1 << 20, &dir).unwrap();
            p.campaign().unwrap();
            settle(&mut p);
            p.propose_conf_change(ConfChangeType::AddLearnerNode, 2, Some(&Command::Put(b"p2".to_vec(), b"a".to_vec())))
                .unwrap();
            p.storage.fail_next(storage::StorageOp::CreateSnapshot);
            assert!(p.drive().is_err(), "the forced compaction was attempted");
        }
        let mut p = Partition::open_bounded(1, vec![7], 1 << 20, &dir).unwrap();
        p.drive().unwrap();
        assert_eq!(voters_and_learners(&p), (vec![1], vec![2]));
        assert_eq!(p.state_get(b"p2").unwrap(), Some(b"a".to_vec()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_lost_voter_id_recreated_empty_is_refused() {
        let root = tmp_dir("recreated-voter");
        let mut nodes: Vec<_> = three_durable(&root)
            .into_iter()
            .map(|p| p.with_snapshot_interval(4))
            .collect();
        nodes[0].campaign().unwrap();
        route(&mut nodes, &[], 50, false);
        for i in 0..12u8 {
            nodes[0].propose(&Command::Put(vec![b'k', i], vec![i])).unwrap();
            route(&mut nodes, &[], 5, false);
        }
        assert!(nodes[0].snapshot_index() > 0);
        drop(nodes.pop());
        let _ = std::fs::remove_dir_all(root.join("3"));
        nodes.push(Partition::open_bounded_joining(3, 1 << 20, root.join("3")).unwrap());
        let mut failure = None;
        'feed: for _ in 0..200 {
            let mut msgs = Vec::new();
            for (i, n) in nodes.iter_mut().enumerate() {
                n.tick();
                match n.drive() {
                    Ok(()) => msgs.append(&mut n.take_outgoing()),
                    Err(e) if i == 2 => {
                        failure = Some(e);
                        break 'feed;
                    }
                    Err(e) => panic!("{e}"),
                }
            }
            for m in msgs {
                let to = (m.to - 1) as usize;
                if let Err(e) = nodes[to].step(m) {
                    assert_eq!(to, 2);
                    failure = Some(e);
                    break 'feed;
                }
            }
        }
        let failure = failure.expect("the recreated voter refused the group");
        assert!(failure.to_string().contains("cannot be recreated"), "{failure}");
        assert!(matches!(nodes[2].drive(), Err(PartitionError::RestartRequired)));
        assert!(root.join("3").join("joining").exists(), "nothing was installed");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_joining_replica_never_votes_and_refuses_a_log_fed_from_index_one() {
        let dir = tmp_dir("join-log-feed");
        let mut joiner = Partition::open_bounded_joining(2, 1 << 20, &dir).unwrap();
        for kind in [MessageType::MsgRequestVote, MessageType::MsgRequestPreVote] {
            let mut vote = Message::default();
            vote.set_msg_type(kind);
            vote.set_from(1);
            vote.set_to(2);
            vote.set_term(7);
            vote.set_index(5);
            vote.set_log_term(6);
            joiner.step(vote).unwrap();
            joiner.drive().unwrap();
            assert!(joiner.take_outgoing().is_empty(), "a replica outside every configuration answered {kind:?}");
        }
        assert_eq!(joiner.term(), 0);
        let mut append = Message::default();
        append.set_msg_type(MessageType::MsgAppend);
        append.set_from(1);
        append.set_to(2);
        append.set_term(2);
        let mut entry = Entry::default();
        entry.set_index(1);
        entry.set_term(2);
        append.set_entries(vec![entry].into());
        let error = joiner.step(append).unwrap_err();
        assert!(error.to_string().contains("cannot be recreated"), "{error}");
        assert!(matches!(joiner.drive(), Err(PartitionError::RestartRequired)));
        drop(joiner);
        let reopened = Partition::open_bounded_joining(2, 1 << 20, &dir).unwrap();
        assert_eq!(reopened.last_index(), 0, "no entry was taken from a log feed");
        assert!(reopened.is_joining());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_removed_voter_cannot_disrupt_the_group() {
        let mut nodes = vec![
            Partition::new_bounded(1, vec![1, 2, 3], 1 << 20).unwrap(),
            Partition::new_bounded(2, vec![1, 2, 3], 1 << 20).unwrap(),
            Partition::new_bounded(3, vec![1, 2, 3], 1 << 20).unwrap(),
        ];
        nodes[0].campaign().unwrap();
        route(&mut nodes, &[], 50, false);
        nodes[0].propose_conf_change(ConfChangeType::RemoveNode, 3, None).unwrap();
        route(&mut nodes, &[3], 50, false);
        assert_eq!(voters_and_learners(&nodes[0]), (vec![1, 2], vec![]));
        assert_eq!(voters_and_learners(&nodes[2]), (vec![1, 2, 3], vec![]), "3 never saw its removal");
        let term = nodes[0].term();
        let stale = nodes[2].term();
        for _ in 0..5 {
            nodes[2].campaign().unwrap();
            route(&mut nodes, &[], 20, false);
        }
        assert_eq!(nodes[2].term(), stale, "pre-vote kept the stale replica from raising its term");
        assert!(nodes[0].is_leader());
        assert_eq!(nodes[0].term(), term, "its vote requests were ignored");
        nodes[0].propose(&Command::Put(b"k".to_vec(), b"v".to_vec())).unwrap();
        route(&mut nodes, &[], 20, false);
        assert_eq!(nodes[1].state_get(b"k").unwrap(), Some(b"v".to_vec()));
        assert_eq!(nodes[2].state_get(b"k").unwrap(), None);

        let follower = &mut nodes[1];
        for _ in 0..10 {
            follower.tick();
        }
        let term = follower.term();
        let (last, last_term) = (follower.last_index(), follower.entry_term(follower.last_index()).unwrap());
        let mut vote = Message::default();
        vote.set_msg_type(MessageType::MsgRequestVote);
        vote.set_from(9);
        vote.set_to(2);
        vote.set_term(term + 5);
        vote.set_index(last);
        vote.set_log_term(last_term);
        follower.step(vote).unwrap();
        assert_eq!(follower.term(), term + 5,
            "a non-member whose log is current (a voter this replica has not seen promoted) is heard");
    }

    #[test]
    fn an_isolated_voter_rejoins_without_deposing_the_leader() {
        let mut nodes = vec![
            Partition::new_bounded(1, vec![1, 2, 3], 1 << 20).unwrap(),
            Partition::new_bounded(2, vec![1, 2, 3], 1 << 20).unwrap(),
            Partition::new_bounded(3, vec![1, 2, 3], 1 << 20).unwrap(),
        ];
        nodes[0].campaign().unwrap();
        route(&mut nodes, &[], 50, true);
        assert!(nodes[0].is_leader());
        let term = nodes[0].term();
        for _ in 0..100 {
            let mut msgs = Vec::new();
            for n in nodes.iter_mut() {
                n.tick();
                n.drive().unwrap();
                msgs.append(&mut n.take_outgoing());
            }
            for m in msgs {
                if m.from == 3 || m.to == 3 {
                    continue;
                }
                nodes[(m.to - 1) as usize].step(m).unwrap();
            }
        }
        assert_eq!(nodes[2].term(), term, "the isolated voter never raised its term");
        assert!(nodes[0].is_leader());
        route(&mut nodes, &[], 50, true);
        assert!(nodes[0].is_leader(), "the rejoining voter deposed the leader");
        assert_eq!(nodes[0].term(), term);
        assert_eq!(nodes[2].leader_id(), Some(1));
    }

    #[test]
    fn a_leader_without_quorum_steps_down() {
        let mut nodes = vec![
            Partition::new_bounded(1, vec![1, 2, 3], 1 << 20).unwrap(),
            Partition::new_bounded(2, vec![1, 2, 3], 1 << 20).unwrap(),
            Partition::new_bounded(3, vec![1, 2, 3], 1 << 20).unwrap(),
        ];
        nodes[0].campaign().unwrap();
        route(&mut nodes, &[], 50, true);
        assert!(nodes[0].is_leader());
        let term = nodes[0].term();
        let mut leader = nodes.remove(0);
        for _ in 0..30 {
            leader.tick();
            leader.drive().unwrap();
            leader.take_outgoing();
        }
        assert!(!leader.is_leader(), "a leader that hears from no quorum steps down");
        assert_eq!(leader.term(), term, "and does not raise its term while alone");
    }

    #[test]
    fn single_node_commits_and_applies() {
        let mut p = Partition::new_single(1, engine()).unwrap();
        settle(&mut p);
        assert!(p.is_leader(), "single node should elect itself");

        p.propose(&Command::Put(b"a".to_vec(), b"1".to_vec()))
            .unwrap();
        p.propose(&Command::Put(b"b".to_vec(), b"22".to_vec()))
            .unwrap();
        settle(&mut p);

        assert_eq!(p.engine().get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
        assert_eq!(p.engine().get(b"b").unwrap().as_deref(), Some(&b"22"[..]));

        p.propose(&Command::Delete(b"a".to_vec())).unwrap();
        settle(&mut p);
        assert_eq!(p.engine().get(b"a").unwrap(), None);
        assert_eq!(p.engine().len(), 1);
    }
}
