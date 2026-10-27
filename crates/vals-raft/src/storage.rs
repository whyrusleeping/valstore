use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use protobuf::Message as _;
use raft::prelude::{ConfState, Entry, HardState, Snapshot};
use raft::storage::{RaftState, Storage};
use raft::{Error as RaftError, GetEntriesContext, StorageError};
use vals_store::crc::crc32;

const STATE_FILE: &str = "state";
const LOG_FILE: &str = "log";
const SNAP_FILE: &str = "snap";
const JOINING_FILE: &str = "joining";

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StorageOp {
    Append,
    HardState,
    Commit,
    ApplySnapshot,
    CreateSnapshot,
}

struct Core {
    hs: HardState,
    cs: ConfState,
    base_cs: ConfState,
    snapshot: Snapshot,
    entries: Vec<Entry>,
    dir: Option<PathBuf>,
    #[cfg(test)]
    failures: std::collections::VecDeque<StorageOp>,
}

impl Core {
    fn snap_index(&self) -> u64 {
        self.snapshot.get_metadata().index
    }

    fn first_index(&self) -> u64 {
        self.snap_index() + 1
    }

    fn last_index(&self) -> u64 {
        self.snap_index() + self.entries.len() as u64
    }

    fn persist_state(&self) -> io::Result<()> {
        let Some(dir) = &self.dir else { return Ok(()) };
        let mut body = Vec::new();
        put_blob(&mut body, &self.hs.write_to_bytes()?);
        put_blob(&mut body, &self.base_cs.write_to_bytes()?);
        persist_file(dir, STATE_FILE, &body)
    }

    fn persist_log(&self) -> io::Result<()> {
        let Some(dir) = &self.dir else { return Ok(()) };
        let mut body = Vec::new();
        body.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for e in &self.entries {
            put_blob(&mut body, &e.write_to_bytes()?);
        }
        persist_file(dir, LOG_FILE, &body)
    }

    fn persist_snap(&self) -> io::Result<()> {
        let Some(dir) = &self.dir else { return Ok(()) };
        persist_file(dir, SNAP_FILE, &self.snapshot.write_to_bytes()?)
    }
}

fn put_blob(out: &mut Vec<u8>, blob: &[u8]) {
    out.extend_from_slice(&(blob.len() as u32).to_le_bytes());
    out.extend_from_slice(blob);
}

fn get_blob<'a>(rest: &mut &'a [u8]) -> io::Result<&'a [u8]> {
    if rest.len() < 4 {
        return Err(corrupt("truncated length prefix"));
    }
    let (len_bytes, after) = rest.split_at(4);
    let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
    if after.len() < len {
        return Err(corrupt("truncated blob"));
    }
    let (blob, tail) = after.split_at(len);
    *rest = tail;
    Ok(blob)
}

fn corrupt(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("pd storage: {what}"))
}

fn persist_file(dir: &Path, name: &str, body: &[u8]) -> io::Result<()> {
    let dst = dir.join(name);
    let tmp = dir.join(format!("{name}.tmp"));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(body)?;
        f.write_all(&crc32(body).to_le_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &dst)?;
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

fn read_file(dir: &Path, name: &str) -> io::Result<Option<Vec<u8>>> {
    let mut bytes = match std::fs::read(dir.join(name)) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    if bytes.len() < 4 {
        return Err(corrupt("file shorter than its checksum"));
    }
    let body_len = bytes.len() - 4;
    let stored = u32::from_le_bytes(bytes[body_len..].try_into().unwrap());
    bytes.truncate(body_len);
    if crc32(&bytes) != stored {
        return Err(corrupt("checksum mismatch"));
    }
    Ok(Some(bytes))
}

#[derive(Clone)]
pub struct PdStorage {
    core: Arc<RwLock<Core>>,
}

impl PdStorage {
    pub fn in_memory(voters: Vec<u64>) -> PdStorage {
        PdStorage {
            core: Arc::new(RwLock::new(Core {
                hs: HardState::default(),
                cs: ConfState::from((voters.clone(), vec![])),
                base_cs: ConfState::from((voters, vec![])),
                snapshot: Snapshot::default(),
                entries: Vec::new(),
                dir: None,
                #[cfg(test)]
                failures: std::collections::VecDeque::new(),
            })),
        }
    }

    pub fn open(dir: impl AsRef<Path>, voters: Vec<u64>) -> io::Result<PdStorage> {
        Self::open_with(dir.as_ref(), voters, false)
    }

    pub fn open_joining(dir: impl AsRef<Path>) -> io::Result<PdStorage> {
        Self::open_with(dir.as_ref(), Vec::new(), true)
    }

    fn open_with(dir: &Path, voters: Vec<u64>, join: bool) -> io::Result<PdStorage> {
        let dir = dir.to_path_buf();
        std::fs::create_dir_all(&dir)?;
        if join && read_file(&dir, STATE_FILE)?.is_none() {
            persist_file(&dir, JOINING_FILE, b"")?;
        }

        let (hs, cs, fresh) = match read_file(&dir, STATE_FILE)? {
            Some(body) => {
                let mut rest = body.as_slice();
                let hs = HardState::parse_from_bytes(get_blob(&mut rest)?)
                    .map_err(|e| corrupt(&format!("hard state: {e}")))?;
                let cs = ConfState::parse_from_bytes(get_blob(&mut rest)?)
                    .map_err(|e| corrupt(&format!("conf state: {e}")))?;
                (hs, cs, false)
            }
            None => (
                HardState::default(),
                ConfState::from((voters, vec![])),
                true,
            ),
        };

        let snapshot = match read_file(&dir, SNAP_FILE)? {
            Some(body) => {
                Snapshot::parse_from_bytes(&body).map_err(|e| corrupt(&format!("snapshot: {e}")))?
            }
            None => Snapshot::default(),
        };

        let mut entries = Vec::new();
        if let Some(body) = read_file(&dir, LOG_FILE)? {
            let mut rest = body.as_slice();
            let count = u32::from_le_bytes(
                get_fixed::<4>(&mut rest)?,
            );
            for _ in 0..count {
                entries.push(
                    Entry::parse_from_bytes(get_blob(&mut rest)?)
                        .map_err(|e| corrupt(&format!("log entry: {e}")))?,
                );
            }
        }
        let snap_index = snapshot.get_metadata().index;
        entries.retain(|e| e.index > snap_index);
        for (i, e) in entries.iter().enumerate() {
            if e.index != snap_index + 1 + i as u64 {
                return Err(corrupt("log entries not contiguous"));
            }
        }

        let cs = if snap_index > 0 {
            snapshot.get_metadata().get_conf_state().clone()
        } else {
            cs
        };
        let mut core = Core {
            hs,
            cs: cs.clone(),
            base_cs: cs,
            snapshot,
            entries,
            dir: Some(dir),
            #[cfg(test)]
            failures: std::collections::VecDeque::new(),
        };
        let mut repair_state = false;
        if core.snap_index() > core.hs.commit {
            core.hs.set_commit(core.snap_index());
            if core.snapshot.get_metadata().term > core.hs.term {
                core.hs.set_term(core.snapshot.get_metadata().term);
            }
            repair_state = true;
        }
        let last = core.last_index();
        if core.hs.commit > last {
            core.hs.commit = last;
            repair_state = true;
        }
        if fresh {
            core.persist_state()?;
            core.persist_snap()?;
            core.persist_log()?;
        } else if repair_state {
            core.persist_state()?;
        }
        Ok(PdStorage {
            core: Arc::new(RwLock::new(core)),
        })
    }

    #[cfg(test)]
    pub(crate) fn fail_next(&self, op: StorageOp) {
        self.core.write().unwrap().failures.push_back(op);
    }

    #[cfg(test)]
    fn inject_failure(core: &mut Core, op: StorageOp) -> io::Result<()> {
        if core.failures.front() == Some(&op) {
            core.failures.pop_front();
            Err(io::Error::other(format!("injected {op:?} failure")))
        } else {
            Ok(())
        }
    }

    pub fn append(&self, ents: &[Entry]) -> io::Result<()> {
        if ents.is_empty() {
            return Ok(());
        }
        let mut core = self.core.write().unwrap();
        if ents[0].index <= core.snap_index() {
            return Err(corrupt("append below the compacted index"));
        }
        if ents[0].index > core.last_index() + 1 {
            return Err(corrupt("append would leave a log gap"));
        }
        let keep = (ents[0].index - core.first_index()) as usize;
        core.entries.truncate(keep);
        core.entries.extend_from_slice(ents);
        #[cfg(test)]
        Self::inject_failure(&mut core, StorageOp::Append)?;
        core.persist_log()
    }

    pub fn set_hardstate(&self, hs: HardState) -> io::Result<()> {
        let mut core = self.core.write().unwrap();
        core.hs = hs;
        #[cfg(test)]
        Self::inject_failure(&mut core, StorageOp::HardState)?;
        core.persist_state()
    }

    pub fn set_commit(&self, commit: u64) -> io::Result<()> {
        let mut core = self.core.write().unwrap();
        core.hs.set_commit(commit);
        #[cfg(test)]
        Self::inject_failure(&mut core, StorageOp::Commit)?;
        core.persist_state()
    }

    pub fn apply_snapshot(&self, snapshot: Snapshot) -> io::Result<bool> {
        let mut core = self.core.write().unwrap();
        let meta = snapshot.get_metadata();
        if meta.index <= core.snap_index() {
            return Ok(false);
        }
        core.hs.set_commit(meta.index);
        if meta.term > core.hs.term {
            core.hs.set_term(meta.term);
        }
        core.cs = meta.get_conf_state().clone();
        core.base_cs = core.cs.clone();
        core.entries.clear();
        core.snapshot = snapshot;
        core.persist_snap()?;
        #[cfg(test)]
        Self::inject_failure(&mut core, StorageOp::ApplySnapshot)?;
        core.persist_state()?;
        core.persist_log()?;
        Ok(true)
    }

    pub fn create_snapshot(&self, index: u64, data: Vec<u8>) -> io::Result<()> {
        let mut core = self.core.write().unwrap();
        if index <= core.snap_index() || index > core.last_index() {
            return Err(corrupt("snapshot index outside the log"));
        }
        let term = core.entries[(index - core.first_index()) as usize].term;
        let mut snapshot = Snapshot::default();
        snapshot.set_data(data.into());
        let meta = snapshot.mut_metadata();
        meta.set_index(index);
        meta.set_term(term);
        meta.set_conf_state(core.cs.clone());
        core.base_cs = core.cs.clone();
        let drop_n = (index - core.snap_index()) as usize;
        core.snapshot = snapshot;
        core.entries.drain(..drop_n);
        core.persist_snap()?;
        #[cfg(test)]
        Self::inject_failure(&mut core, StorageOp::CreateSnapshot)?;
        core.persist_log()
    }

    pub fn set_conf_state(&self, cs: ConfState) {
        self.core.write().unwrap().cs = cs;
    }

    pub fn conf_state(&self) -> ConfState {
        self.core.read().unwrap().cs.clone()
    }

    pub fn is_joining(&self) -> bool {
        let core = self.core.read().unwrap();
        core.dir.as_ref().is_some_and(|dir| dir.join(JOINING_FILE).exists())
    }

    pub fn finish_join(&self) -> io::Result<()> {
        let core = self.core.read().unwrap();
        let Some(dir) = &core.dir else { return Ok(()) };
        match std::fs::remove_file(dir.join(JOINING_FILE)) {
            Ok(()) => std::fs::File::open(dir)?.sync_all(),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub fn snapshot_index(&self) -> u64 {
        self.core.read().unwrap().snap_index()
    }

    pub fn snapshot_data(&self) -> Vec<u8> {
        self.core.read().unwrap().snapshot.get_data().to_vec()
    }

    pub fn hard_state(&self) -> HardState {
        self.core.read().unwrap().hs.clone()
    }
}

fn get_fixed<const N: usize>(rest: &mut &[u8]) -> io::Result<[u8; N]> {
    if rest.len() < N {
        return Err(corrupt("truncated field"));
    }
    let (head, tail) = rest.split_at(N);
    *rest = tail;
    Ok(head.try_into().unwrap())
}

impl Storage for PdStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        let core = self.core.read().unwrap();
        Ok(RaftState::new(core.hs.clone(), core.cs.clone()))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        let core = self.core.read().unwrap();
        if low < core.first_index() {
            return Err(RaftError::Store(StorageError::Compacted));
        }
        if high > core.last_index() + 1 {
            return Err(RaftError::Store(StorageError::Unavailable));
        }
        let offset = core.first_index();
        let slice = &core.entries[(low - offset) as usize..(high - offset) as usize];
        let mut out = Vec::new();
        let mut size = 0u64;
        let cap = max_size.into().unwrap_or(u64::MAX);
        for e in slice {
            size += e.compute_size() as u64;
            if !out.is_empty() && size > cap {
                break;
            }
            out.push(e.clone());
        }
        Ok(out)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let core = self.core.read().unwrap();
        if idx == core.snap_index() {
            return Ok(core.snapshot.get_metadata().term);
        }
        if idx < core.first_index() {
            return Err(RaftError::Store(StorageError::Compacted));
        }
        if idx > core.last_index() {
            return Err(RaftError::Store(StorageError::Unavailable));
        }
        Ok(core.entries[(idx - core.first_index()) as usize].term)
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.core.read().unwrap().first_index())
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.core.read().unwrap().last_index())
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        let core = self.core.read().unwrap();
        if core.snap_index() < request_index {
            return Err(RaftError::Store(
                StorageError::SnapshotTemporarilyUnavailable,
            ));
        }
        Ok(core.snapshot.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: u64, term: u64, data: &[u8]) -> Entry {
        let mut e = Entry::default();
        e.set_index(index);
        e.set_term(term);
        e.set_data(data.to_vec().into());
        e
    }

    fn tmp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("vals-raft-storage-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn state_log_and_snapshot_survive_reopen() {
        let dir = tmp_dir("reopen");
        {
            let s = PdStorage::open(&dir, vec![1, 2, 3]).unwrap();
            s.append(&[entry(1, 1, b"a"), entry(2, 1, b"b"), entry(3, 2, b"c")])
                .unwrap();
            let mut hs = HardState::default();
            hs.set_term(7);
            hs.set_vote(2);
            hs.set_commit(3);
            s.set_hardstate(hs).unwrap();
        }
        let s = PdStorage::open(&dir, vec![9]).unwrap();
        let hs = s.hard_state();
        assert_eq!((hs.term, hs.vote, hs.commit), (7, 2, 3));
        let state = s.initial_state().unwrap();
        assert_eq!(
            state.conf_state.voters,
            vec![1, 2, 3],
            "persisted ConfState wins"
        );
        assert_eq!(s.last_index().unwrap(), 3);
        assert_eq!(s.term(3).unwrap(), 2);
        let ents = s
            .entries(1, 4, None, GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(ents.len(), 3);
        assert_eq!(ents[2].get_data(), b"c");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn applied_conf_state_is_not_the_restart_base_until_compacted() {
        let dir = tmp_dir("conf-base");
        {
            let s = PdStorage::open(&dir, vec![1]).unwrap();
            s.append(&[entry(1, 1, b"a"), entry(2, 1, b"")]).unwrap();
            s.set_conf_state(ConfState::from((vec![1], vec![2])));
            s.set_commit(2).unwrap();
        }
        {
            let s = PdStorage::open(&dir, vec![9]).unwrap();
            assert_eq!(s.initial_state().unwrap().conf_state.learners, Vec::<u64>::new(),
                "replay from index 1 re-applies the change on the initial configuration");
            s.set_conf_state(ConfState::from((vec![1], vec![2])));
            s.create_snapshot(2, b"dump".to_vec()).unwrap();
        }
        let s = PdStorage::open(&dir, vec![9]).unwrap();
        assert_eq!(s.initial_state().unwrap().conf_state.learners, vec![2]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_fresh_joining_directory_has_no_configuration_and_a_marker() {
        let dir = tmp_dir("joining");
        {
            let s = PdStorage::open_joining(&dir).unwrap();
            assert!(s.is_joining());
            assert!(s.initial_state().unwrap().conf_state.voters.is_empty());
        }
        let s = PdStorage::open_joining(&dir).unwrap();
        assert!(s.is_joining(), "the marker survives a restart before the first snapshot");
        s.finish_join().unwrap();
        assert!(!s.is_joining());
        assert!(!PdStorage::open_joining(&dir).unwrap().is_joining(), "an existing directory never re-joins");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn conflicting_append_truncates_the_suffix() {
        let s = PdStorage::in_memory(vec![1]);
        s.append(&[entry(1, 1, b"a"), entry(2, 1, b"b"), entry(3, 1, b"c")])
            .unwrap();
        s.append(&[entry(2, 2, b"B")]).unwrap();
        assert_eq!(s.last_index().unwrap(), 2);
        assert_eq!(s.term(2).unwrap(), 2);
        assert!(s.append(&[entry(9, 2, b"z")]).is_err());
    }

    #[test]
    fn create_snapshot_compacts_and_recovers() {
        let dir = tmp_dir("snap");
        {
            let s = PdStorage::open(&dir, vec![1]).unwrap();
            s.append(&[entry(1, 1, b"a"), entry(2, 1, b"b"), entry(3, 1, b"c")])
                .unwrap();
            s.set_commit(3).unwrap();
            s.create_snapshot(2, b"dump".to_vec()).unwrap();
            assert_eq!(s.first_index().unwrap(), 3, "log compacted through 2");
            assert_eq!(s.term(2).unwrap(), 1, "truncation-point term retained");
            assert!(matches!(
                s.entries(1, 2, None, GetEntriesContext::empty(false)),
                Err(RaftError::Store(StorageError::Compacted))
            ));
        }
        let s = PdStorage::open(&dir, vec![1]).unwrap();
        assert_eq!(s.snapshot_index(), 2);
        assert_eq!(s.snapshot_data(), b"dump");
        assert_eq!(s.last_index().unwrap(), 3, "suffix after the snapshot kept");
        let snap = s.snapshot(0, 1).unwrap();
        assert_eq!(snap.get_metadata().index, 2);
        assert_eq!(snap.get_data(), b"dump");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_file_is_refused() {
        let dir = tmp_dir("corrupt");
        {
            let s = PdStorage::open(&dir, vec![1]).unwrap();
            s.append(&[entry(1, 1, b"a")]).unwrap();
        }
        let path = dir.join(LOG_FILE);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[4] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();
        assert!(PdStorage::open(&dir, vec![1]).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
