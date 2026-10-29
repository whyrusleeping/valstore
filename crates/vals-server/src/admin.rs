use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::mpsc;

use vals_cluster::pd::{
    check_membership_change, AdminMutation, AdminOutcome, MembershipChange, PdDaemon, PartitionError, SplitMode,
};
use vals_cluster::zone::{ZoneConfig, ZoneId};
use vals_cluster::Ordering;

pub(crate) const COMMANDS: &str = "ping status members zones ranges export-state add-node remove-node \
     create-zone drop-zone split-zone merge-zone pd-add-learner pd-remove-learner pd-replace";

struct AdminResponse {
    text: String,
    _byte_credit: vals_cluster::transport::ByteCredit,
}

#[derive(Clone)]
pub struct AdminReply(mpsc::Sender<AdminResponse>);

impl AdminReply {
    pub fn send(&self, text: String) -> Result<(), String> {
        let Some(byte_credit) =
            vals_cluster::transport::acquire_queued_bytes(text.capacity().saturating_add(1))
        else {
            vals_cluster::observe::counters().transport_queued_byte_rejections.inc();
            return Err(text);
        };
        self.0
            .send(AdminResponse {
                text,
                _byte_credit: byte_credit,
            })
            .map_err(|e| e.0.text)
    }
}

pub type AdminReq = (String, AdminReply);

pub fn spawn_listener(path: &Path) -> std::io::Result<mpsc::Receiver<AdminReq>> {
    let _ = std::fs::remove_file(path);
    let listener = UnixListener::bind(path)?;
    let (tx, rx) = mpsc::channel::<AdminReq>();
    let sock_path = path.to_path_buf();
    vals_cluster::observe::spawn_abort_on_panic("vals-admin", move || {
        for conn in listener.incoming() {
            let Ok(conn) = conn else { continue };
            let Some(connection_credit) = vals_cluster::transport::acquire_connection() else {
                vals_cluster::observe::counters().transport_connection_rejections.inc();
                continue;
            };
            let tx = tx.clone();
            let spawned = std::thread::Builder::new()
                .name("vals-admin-conn".into())
                .spawn(move || serve_conn(conn, &tx, connection_credit));
            if spawned.is_err() {
                continue;
            }
        }
        log::warn!("admin socket {} closed", sock_path.display());
    });
    Ok(rx)
}

fn serve_conn(
    mut conn: UnixStream,
    tx: &mpsc::Sender<AdminReq>,
    _connection_credit: vals_cluster::transport::Credit,
) {
    const MAX_COMMAND: usize = 64 * 1024;
    let limits = vals_cluster::transport::limits();
    if conn.set_write_timeout(limits.write_timeout).is_err() {
        return;
    }
    let Some(_input_credit) = vals_cluster::transport::acquire_queued_bytes(MAX_COMMAND) else {
        vals_cluster::observe::counters().transport_queued_byte_rejections.inc();
        return;
    };
    let deadline = limits.initial_timeout.map(|d| std::time::Instant::now() + d);
    let mut input = Vec::with_capacity(MAX_COMMAND);
    let mut chunk = [0u8; 1024];
    loop {
        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                vals_cluster::observe::counters().transport_initial_timeouts.inc();
                return;
            }
            if conn.set_read_timeout(Some(remaining)).is_err() {
                return;
            }
        }
        let room = MAX_COMMAND - input.len();
        let read_len = room.min(chunk.len());
        match conn.read(&mut chunk[..read_len]) {
            Ok(0) => break,
            Ok(n) => {
                input.extend_from_slice(&chunk[..n]);
                if let Some(end) = input.iter().position(|&b| b == b'\n') {
                    input.truncate(end + 1);
                    break;
                }
                if input.len() == MAX_COMMAND {
                    return;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                ) =>
            {
                vals_cluster::observe::counters().transport_initial_timeouts.inc();
                return;
            }
            Err(_) => return,
        }
    }
    let Ok(mut line) = String::from_utf8(input) else {
        return;
    };
    let (trim_start, trim_end) = {
        let trimmed = line.trim();
        let start = trimmed.as_ptr() as usize - line.as_ptr() as usize;
        (start, start + trimmed.len())
    };
    line.truncate(trim_end);
    line.drain(..trim_start);
    let Some(_request_credit) = vals_cluster::transport::acquire_request() else {
        vals_cluster::observe::counters().transport_request_rejections.inc();
        return;
    };
    let (reply_tx, reply_rx) = mpsc::channel();
    if tx.send((line, AdminReply(reply_tx))).is_err() {
        return;
    }
    let reply = match reply_rx.recv() {
        Ok(reply) => reply,
        Err(_) => return,
    };
    let written = conn
        .write_all(reply.text.as_bytes())
        .and_then(|_| conn.write_all(b"\n"));
    if let Err(e) = written {
        if matches!(e.kind(), std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock) {
            vals_cluster::observe::counters().transport_write_timeouts.inc();
        }
    }
}

pub fn handle_command(pd: &PdDaemon, line: &str) -> String {
    match try_handle(pd, line) {
        Ok(reply) => reply,
        Err(msg) if msg.starts_with("unknown: ") => msg,
        Err(msg) => format!("err: {msg}"),
    }
}

fn try_handle(pd: &PdDaemon, line: &str) -> Result<String, String> {
    let mut parts: Vec<&str> = line.split_whitespace().collect();
    let force = parts.last() == Some(&"--force");
    if force {
        parts.pop();
        if !matches!(parts.first(), Some(&"add-node" | &"remove-node")) {
            return Err("--force applies only to add-node and remove-node".into());
        }
    }
    match parts.as_slice() {
        ["help"] => Ok(format!("ok admin commands: help {COMMANDS}")),
        ["ping"] => Ok("ok".into()),
        ["status"] => {
            let s = pd.status().ok_or("pd status unavailable")?;
            Ok(format!(
                "ok pd={} role={} term={} leader={} commit={} applied={} caught_up={} incarnation={} \
                 voters={} learners={}",
                s.id, s.role, s.term, leader_name(s.leader), s.commit, s.applied, s.caught_up,
                s.incarnation.map_or_else(|| "-".to_string(), |id| format!("{id:032x}")),
                id_list(&s.voters), id_list(&s.learners),
            ))
        }

        ["members"] => {
            let view = pd.inspect().ok_or("pd inspection unavailable")?;
            let mut out = inspection_header(&view);
            for (id, weight, addrs) in view.members {
                let addrs = if addrs.is_empty() {
                    "-".to_string()
                } else {
                    addrs
                        .iter()
                        .map(|a| a.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                };
                out.push_str(&format!("\n{id} {weight} {addrs}"));
            }
            Ok(out)
        }

        ["zones"] => {
            let view = pd.inspect().ok_or("pd inspection unavailable")?;
            let mut out = inspection_header(&view);
            let catalog = &view.zones;
            for id in catalog.ids() {
                let cfg = catalog.get(id).expect("listed id");
                let ord = match cfg.ordering {
                    Ordering::Sorted => "sorted",
                    Ordering::Unsorted => "unsorted",
                };
                out.push_str(&format!("\n{id} {} {} {ord}", cfg.name, cfg.rf));
            }
            Ok(out)
        }

        ["ranges", zone] => {
            let view = pd.inspect().ok_or("pd inspection unavailable")?;
            let id = zone
                .parse::<ZoneId>()
                .ok()
                .or_else(|| view.zones.by_name(zone))
                .ok_or_else(|| format!("no committed zone named {zone:?}"))?;
            if !view.zones.contains(id) {
                return Err(format!("no committed zone {id}"));
            }
            let mut out = inspection_header(&view);
            if let Some(map) = view.range_maps.get(&id) {
                for key in map.splits() {
                    out.push_str("\nhex=");
                    for byte in key {
                        out.push_str(&format!("{byte:02x}"));
                    }
                }
            }
            Ok(out)
        }

        ["export-state", path] => {
            let path = Path::new(path);
            if !path.is_absolute() {
                return Err("export-state needs an absolute path: the PD process writes the file".into());
            }
            let view = pd.capture_state().ok_or("pd inspection unavailable")?;
            let pd_id = view.status.id;
            let capture = vals_cluster::backup::capture_from_inspection(view)?;
            let bytes = capture.encode();
            vals_cluster::backup::write_new_file_synced(path, &bytes)
                .map_err(|e| format!("write {}: {e}", path.display()))?;
            Ok(format!(
                "ok authoritative pd={} term={} index={} entries={} bytes={} incarnation={}",
                pd_id, capture.term, capture.applied, capture.entries.len(),
                bytes.len(),
                capture.view().incarnation.map_or_else(|| "-".to_string(), |id| format!("{id:032x}")),
            ))
        }

        ["add-node", id, weight] => {
            let id = parse(id, "node id")?;
            let weight = parse(weight, "weight")?;
            let gate = membership_gate(pd, MembershipChange::Add(id), &[], force)?;
            proposed(pd, AdminMutation::Add(id, weight, Vec::new())).map(|ok| format!("{ok}; {gate}"))
        }
        ["add-node", id, weight, addrs] => {
            let id = parse(id, "node id")?;
            let weight = parse(weight, "weight")?;
            let addrs = addrs
                .split(',')
                .map(|a| a.parse().map_err(|_| format!("bad address {a:?}")))
                .collect::<Result<Vec<_>, _>>()?;
            let gate = membership_gate(pd, MembershipChange::Add(id), &addrs, force)?;
            proposed(pd, AdminMutation::Add(id, weight, addrs)).map(|ok| format!("{ok}; {gate}"))
        }

        ["remove-node", id] => {
            let id = parse(id, "node id")?;
            let gate = membership_gate(pd, MembershipChange::Remove(id), &[], force)?;
            proposed(pd, AdminMutation::Remove(id)).map(|ok| format!("{ok}; {gate}"))
        }

        ["create-zone", name, rf, ordering] => {
            let ordering = parse_ordering(ordering)?;
            proposed(pd, AdminMutation::CreateZone(
                ZoneConfig {
                    name: name.to_string(),
                    rf: parse(rf, "rf")?,
                    ordering,
                    scope: None,
                },
                Vec::new(),
            ))
        }
        ["create-zone", name, rf, ordering, presplit] => {
            let ordering = parse_ordering(ordering)?;
            if ordering != Ordering::Sorted {
                return Err("presplit is only valid for a sorted zone".into());
            }
            let value = presplit
                .strip_prefix("presplit=")
                .ok_or_else(|| {
                    format!("expected presplit=<K> or presplit=<key,key,...>, got {presplit:?}")
                })?;
            let boundaries = parse_presplit(value)?;
            proposed(pd, AdminMutation::CreateZone(
                ZoneConfig {
                    name: name.to_string(),
                    rf: parse(rf, "rf")?,
                    ordering,
                    scope: None,
                },
                boundaries,
            ))
        }

        ["drop-zone", zone] => proposed(pd, AdminMutation::DropZone(resolve_zone(pd, zone)?)),

        ["split-zone", zone, key] => proposed(pd, AdminMutation::SplitZone(
            resolve_zone(pd, zone)?, key.as_bytes().to_vec(), SplitMode::Inherit,
        )),
        ["split-zone", zone, key, "canonical"] => proposed(pd, AdminMutation::SplitZone(
            resolve_zone(pd, zone)?, key.as_bytes().to_vec(), SplitMode::Canonical,
        )),
        ["split-zone", _, _, mode] => Err(format!(
            "expected split-zone <zone> <key> [canonical], got mode {mode:?}"
        )),

        ["merge-zone", zone, key] => proposed(pd, AdminMutation::MergeZone(
            resolve_zone(pd, zone)?, key.as_bytes().to_vec(),
        )),

        ["pd-add-learner", id, addr] => {
            let id: u64 = parse(id, "pd id")?;
            let addr = parse(addr, "pd peer address")?;
            let reply = proposed(pd, AdminMutation::AddPdLearner(id, addr))?;
            confirm_membership(pd, reply, |_, learners| learners.contains(&id), &format!("pd {id} is a learner"))
        }
        ["pd-remove-learner", id] => {
            let id: u64 = parse(id, "pd id")?;
            let reply = proposed(pd, AdminMutation::RemovePdLearner(id))?;
            confirm_membership(pd, reply, |voters, learners| {
                !learners.contains(&id) && !voters.contains(&id)
            }, &format!("pd {id} is no longer a member"))
        }
        ["pd-replace", old, new] => {
            let old: u64 = parse(old, "pd id")?;
            let new: u64 = parse(new, "pd id")?;
            replace_pd_voter(pd, old, new)
        }

        [] => Err("empty command".into()),
        [cmd, ..] => Err(format!("unknown command {cmd:?} (admin commands: help {COMMANDS})")),
    }
}

fn membership_gate(
    pd: &PdDaemon,
    change: MembershipChange,
    incoming: &[std::net::SocketAddr],
    force: bool,
) -> Result<String, String> {
    if force {
        return Ok(match change {
            MembershipChange::Add(_) => "cleanup gate skipped (--force)",
            MembershipChange::Remove(_) => "surviving-replica and cleanup gates skipped (--force)",
        }
        .into());
    }
    let view = pd.inspect().ok_or("pd inspection unavailable")?;
    if let MembershipChange::Remove(node) = change {
        refuse_sole_copy_removal(&view, node)?;
    }
    check_membership_change(&view.members, &view.range_maps, change, incoming)
        .map(|settled| format!("cleanup gate: {settled}"))
        .map_err(|why| {
            format!(
                "membership change refused, nothing applied: {why}. A node that becomes an owner serves the \
                 copies it holds, so a copy whose delete was collected would come back. Wait for cleanup to \
                 settle; delete a key that should stay deleted (cleanup then releases its copy); wipe a node \
                 before it joins; or rerun with --force to accept that risk \
                 (docs/backup-restore.md#consistency-contract)"
            )
        })
}

fn refuse_sole_copy_removal(
    view: &vals_cluster::pd::PdInspection,
    node: vals_cluster::NodeId,
) -> Result<(), String> {
    if !view.members.iter().any(|(member, _, _)| *member == node) {
        return Ok(());
    }
    let mut zones = Vec::new();
    if view.ring_rf == 1 {
        zones.push("the default zone (ring rf 1)".to_string());
    }
    for id in view.zones.ids() {
        let cfg = view.zones.get(id).expect("listed id");
        if cfg.rf == 1 && cfg.scope.as_ref().is_none_or(|scope| scope.contains(&node)) {
            zones.push(format!("zone {id} ({}, rf 1)", cfg.name));
        }
    }
    if zones.is_empty() {
        return Ok(());
    }
    Err(format!(
        "membership change refused, nothing applied: removing node {node} would leave keys with no \
         surviving replica in {}. remove-node does not copy a node's keys to their new owners first, \
         so the keys it holds there would read as not found. Move nodes of an rf 1 zone with backup \
         and restore, or rerun with --force to accept losing access to those keys \
         (docs/growing.md#adding-data-nodes)",
        zones.join(", ")
    ))
}

fn id_list(ids: &[u64]) -> String {
    if ids.is_empty() {
        return "-".into();
    }
    ids.iter().map(u64::to_string).collect::<Vec<_>>().join(",")
}

fn confirm_membership(
    pd: &PdDaemon,
    reply: String,
    reflected: impl Fn(&[u64], &[u64]) -> bool,
    what: &str,
) -> Result<String, String> {
    let s = pd.status().ok_or("pd status unavailable after the change")?;
    if reflected(&s.voters, &s.learners) {
        Ok(format!("{reply}; voters={} learners={}", id_list(&s.voters), id_list(&s.learners)))
    } else {
        Err(format!(
            "{reply}, but not reflected: expected {what}, have voters={} learners={}; see the PD log",
            id_list(&s.voters), id_list(&s.learners)
        ))
    }
}

fn replace_pd_voter(pd: &PdDaemon, old: u64, new: u64) -> Result<String, String> {
    let mut steps = Vec::new();
    for _ in 0..3 {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let outcome = loop {
            match pd.admin_submit(AdminMutation::ReplacePdVoter(old, new)) {
                Err(PartitionError::NotReady) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
                outcome => break outcome,
            }
        };
        match outcome {
            Ok(AdminOutcome::Unchanged) => {
                let s = pd.status().ok_or("pd status unavailable")?;
                if !s.voters.contains(&new) || s.voters.contains(&old) {
                    return Err(format!(
                        "replacement not reflected: voters={} learners={}",
                        id_list(&s.voters), id_list(&s.learners)
                    ));
                }
                let done = if steps.is_empty() { "already done".to_string() } else { steps.join(", ") };
                return Ok(format!(
                    "ok pd {old} replaced by pd {new} ({done}); voters={} learners={}",
                    id_list(&s.voters), id_list(&s.learners)
                ));
            }
            Ok(AdminOutcome::Committed { index, term }) => {
                steps.push(format!("committed index={index} term={term}"));
            }
            Ok(other) => return Err(format!("unexpected outcome {other:?}")),
            Err(error) => {
                let progress = if steps.is_empty() {
                    String::new()
                } else {
                    format!("after {}: ", steps.join(", "))
                };
                let reply = proposed_error(pd, error);
                let (prefix, rest) = match reply.strip_prefix("unknown: ") {
                    Some(rest) => ("unknown: ", rest),
                    None => ("", reply.as_str()),
                };
                return Err(format!(
                    "{prefix}{progress}{rest}; rerun pd-replace {old} {new} to continue"
                ));
            }
        }
    }
    Err(format!("pd-replace {old} {new} made no final progress; inspect status and rerun"))
}

fn leader_name(leader: Option<u64>) -> String {
    leader.map_or_else(|| "unknown".into(), |id| id.to_string())
}

fn inspection_header(view: &vals_cluster::pd::PdInspection) -> String {
    let s = &view.status;
    if let Some(index) = view.authoritative {
        format!("ok authoritative pd={} term={} index={index}", s.id, s.term)
    } else {
        format!(
            "ok non-authoritative pd={} role={} leader={} applied={} (may lag; not confirmation)",
            s.id, s.role, leader_name(s.leader), s.applied,
        )
    }
}

fn proposed(pd: &PdDaemon, mutation: AdminMutation) -> Result<String, String> {
    match pd.admin_submit(mutation) {
        Ok(AdminOutcome::Committed { index, term }) => {
            Ok(format!("ok committed index={index} term={term}"))
        }
        Ok(AdminOutcome::Unchanged) => Ok("ok unchanged: already in effect".into()),
        Ok(AdminOutcome::CommittedMapQueued { index, term }) => Ok(format!(
            "ok committed index={index} term={term}; initial range map queued, NOT committed; confirm with ranges <zone> on authoritative leader"
        )),
        Ok(AdminOutcome::CommittedMapDropped { index, term }) => Ok(format!(
            "ok committed index={index} term={term}; initial range map NOT queued; inspect ranges <zone> on authoritative leader"
        )),
        Ok(AdminOutcome::Queued) => Ok(
            "ok queued, NOT committed; confirm with ranges <zone> on authoritative leader".into(),
        ),
        Err(error) => Err(proposed_error(pd, error)),
    }
}

fn proposed_error(pd: &PdDaemon, error: PartitionError) -> String {
    match error {
        PartitionError::Unknown { index, term } => format!(
            "unknown: commit not observed index={index} term={term}; inspect authoritative leader before retrying"
        ),
        PartitionError::AdminNoReply => {
            "unknown: no reply from PD drive loop within timeout; inspect authoritative leader before retrying".into()
        }
        error @ (PartitionError::NotLeader | PartitionError::NotReady) => {
            let Some(s) = pd.status() else { return error.to_string() };
            let leader = s.leader.map_or_else(
                || "leader unknown".into(),
                |id| format!("pd {id} is leader"),
            );
            let kind = if matches!(error, PartitionError::NotLeader) {
                "not leader"
            } else {
                "not ready"
            };
            format!(
                "{kind}: {leader} (term {}); nothing applied; send mutations to that replica's admin socket",
                s.term,
            )
        }
        error => error.to_string(),
    }
}

fn parse<T: std::str::FromStr>(s: &str, what: &str) -> Result<T, String> {
    s.parse().map_err(|_| format!("bad {what}: {s:?}"))
}

fn parse_ordering(ordering: &str) -> Result<Ordering, String> {
    match ordering {
        "sorted" => Ok(Ordering::Sorted),
        "unsorted" => Ok(Ordering::Unsorted),
        other => Err(format!("expected sorted|unsorted, got {other:?}")),
    }
}

fn parse_presplit(value: &str) -> Result<Vec<Vec<u8>>, String> {
    if let Ok(k) = value.parse::<u32>() {
        if !(2..=256).contains(&k) {
            return Err(
                "presplit K must be in 2..=256; a purely numeric boundary key must be added later with split-zone <zone> <key> canonical".into(),
            );
        }
        return Ok(vals_cluster::even_boundaries(k));
    }

    let boundaries: Vec<Vec<u8>> = value
        .split(',')
        .map(|key| key.as_bytes().to_vec())
        .collect();
    if boundaries.iter().any(Vec::is_empty) {
        return Err("presplit boundary keys must be non-empty".into());
    }
    if boundaries.len() > 255 {
        return Err("presplit accepts at most 255 boundary keys (256 ranges)".into());
    }
    if boundaries
        .windows(2)
        .any(|pair| pair[0].as_slice() >= pair[1].as_slice())
    {
        return Err("presplit boundary keys must be strictly increasing in byte order".into());
    }
    Ok(boundaries)
}

fn resolve_zone(pd: &PdDaemon, arg: &str) -> Result<ZoneId, String> {
    if let Ok(id) = arg.parse::<ZoneId>() {
        return Ok(id);
    }
    pd.catalog()
        .by_name(arg)
        .ok_or_else(|| format!("no committed zone named {arg:?}"))
}

pub fn client(socket: &Path, cmd: &[String]) -> Result<u8, std::io::Error> {
    let mut conn = UnixStream::connect(socket)?;
    conn.set_read_timeout(Some(std::time::Duration::from_secs(7)))?;
    conn.set_write_timeout(Some(std::time::Duration::from_secs(7)))?;
    let mut sent = false;
    let result = (|| {
        conn.write_all(cmd.join(" ").as_bytes())?;
        sent = true;
        conn.write_all(b"\n")?;
        conn.shutdown(std::net::Shutdown::Write)?;
        let mut reply = String::new();
        conn.read_to_string(&mut reply)?;
        Ok::<_, std::io::Error>(reply)
    })();
    match result {
        Ok(reply) if reply.ends_with('\n') => {
            print!("{reply}");
            Ok(reply_exit(&reply))
        }
        Ok(_) | Err(_) if sent && !matches!(cmd.first().map(String::as_str),
            Some("help" | "ping" | "status" | "members" | "zones" | "ranges" | "export-state")) => {
            println!("unknown: no complete reply after sending command; inspect authoritative leader before retrying");
            Ok(3)
        }
        Ok(_) => Ok(1),
        Err(error) => Err(error),
    }
}

pub fn request(socket: &Path, line: &str) -> std::io::Result<String> {
    let mut conn = UnixStream::connect(socket)?;
    conn.set_read_timeout(Some(std::time::Duration::from_secs(30)))?;
    conn.set_write_timeout(Some(std::time::Duration::from_secs(7)))?;
    conn.write_all(line.as_bytes())?;
    conn.write_all(b"\n")?;
    conn.shutdown(std::net::Shutdown::Write)?;
    let mut reply = String::new();
    conn.read_to_string(&mut reply)?;
    if !reply.ends_with('\n') {
        return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "incomplete admin reply"));
    }
    reply.pop();
    Ok(reply)
}

fn reply_exit(reply: &str) -> u8 {
    if reply.starts_with("ok") { 0 } else if reply.starts_with("unknown:") { 3 } else { 1 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};
    use vals_cluster::pd::RingConfig;
    use vals_spdk::MemBlockDevice;
    use vals_store::{Config, Engine};

    #[test]
    fn admin_deadline_and_queued_command_credits_are_bounded() {
        const CHILD: &str = "VALS_ADMIN_TRANSPORT_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("admin_deadline_and_queued_command_credits_are_bounded")
                .arg("--nocapture")
                .env(CHILD, "1")
                .env("VALS_TRANSPORT_UNBOUNDED", "0")
                .env("VALS_TRANSPORT_MAX_CONNECTIONS", "4")
                .env("VALS_TRANSPORT_MAX_REQUESTS", "1")
                .env("VALS_TRANSPORT_MAX_QUEUED_BYTES", "131072")
                .env("VALS_TRANSPORT_INITIAL_TIMEOUT_MS", "100")
                .env("VALS_TRANSPORT_FRAME_TIMEOUT_MS", "100")
                .env("VALS_TRANSPORT_IDLE_TIMEOUT_MS", "100")
                .env("VALS_TRANSPORT_WRITE_TIMEOUT_MS", "100")
                .status()
                .unwrap();
            assert!(status.success(), "isolated admin transport test failed: {status}");
            return;
        }

        vals_cluster::transport::configure(
            vals_cluster::transport::TransportLimits::from_env().unwrap(),
        )
        .unwrap();
        let (tx, _rx) = mpsc::channel();
        let (server, mut client) = UnixStream::pair().unwrap();
        let conn = vals_cluster::transport::acquire_connection().unwrap();
        let slow = std::thread::spawn(move || serve_conn(server, &tx, conn));
        client.set_write_timeout(Some(Duration::from_secs(1))).unwrap();
        for byte in b"slow command\n" {
            if client.write_all(&[*byte]).is_err() {
                break;
            }
            std::thread::sleep(Duration::from_millis(40));
        }
        slow.join().unwrap();
        assert!(vals_cluster::observe::snapshot().transport_initial_timeouts >= 1);

        let (tx, rx) = mpsc::channel();
        let (server, mut client) = UnixStream::pair().unwrap();
        let conn = vals_cluster::transport::acquire_connection().unwrap();
        let queued = std::thread::spawn(move || serve_conn(server, &tx, conn));
        client.write_all(b"ping\n").unwrap();
        let request = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(vals_cluster::transport::acquire_request().is_none());
        assert_eq!(vals_cluster::transport::usage().2, 64 * 1024);
        drop(request);
        queued.join().unwrap();
        assert_eq!(vals_cluster::transport::usage(), (0, 0, 0));
    }

    fn spawn_pd() -> PdDaemon {
        let engine =
            Engine::create_prewiped(MemBlockDevice::new(4096, 16 * 1024), Config::default());
        let pd = PdDaemon::spawn_single(
            1,
            engine,
            RingConfig { vnodes: 128, rf: 1 },
            Vec::new(),
        )
        .unwrap();
        pd.add_node(1).unwrap();
        pd
    }

    #[test]
    fn status_inspection_and_reply_classes() {
        assert_eq!(reply_exit("ok committed index=4 term=1\n"), 0);
        assert_eq!(reply_exit("err: not leader\n"), 1);
        assert_eq!(reply_exit("unknown: commit not observed\n"), 3);
        assert_eq!(reply_exit("unknown: no reply from PD drive loop\n"), 3);
        let pd = spawn_pd();
        let status = handle_command(&pd, "status");
        assert!(status.starts_with("ok pd=1 role=leader term="), "{status}");
        let members = handle_command(&pd, "members");
        assert!(members.starts_with("ok authoritative pd=1 term="), "{members}");
        assert!(members.ends_with("\n1 1 -"), "{members}");
        let zone = handle_command(&pd, "create-zone s 1 sorted");
        assert!(zone.starts_with("ok committed index="), "{zone}");
        let ranges = handle_command(&pd, "ranges s");
        assert!(ranges.starts_with("ok authoritative pd=1 term="), "{ranges}");
        let split = handle_command(&pd, "split-zone s m");
        assert!(split.starts_with("ok queued, NOT committed"), "{split}");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let ranges = handle_command(&pd, "ranges s");
            if ranges.ends_with("\nhex=6d") { break; }
            assert!(Instant::now() < deadline, "range not committed: {ranges}");
            std::thread::sleep(Duration::from_millis(10));
        }
        pd.shutdown();
    }

    #[test]
    fn membership_changes_need_a_settled_cleanup_or_force() {
        let pd = spawn_pd();
        let refused = handle_command(&pd, "add-node 2 1");
        assert!(refused.starts_with("err: membership change refused, nothing applied"), "{refused}");
        assert!(refused.contains("node 1 has no data address"), "{refused}");
        assert!(handle_command(&pd, "members").ends_with("\n1 1 -"));
        let forced = handle_command(&pd, "add-node 2 1 --force");
        assert!(forced.starts_with("ok committed index="), "{forced}");
        assert!(forced.ends_with("cleanup gate skipped (--force)"), "{forced}");
        assert_eq!(handle_command(&pd, "members --force"), "err: --force applies only to add-node and remove-node");
        pd.shutdown();
    }

    #[test]
    fn remove_node_refuses_to_leave_keys_without_a_replica_unless_forced() {
        let pd = spawn_pd();
        assert!(handle_command(&pd, "add-node 2 1 --force").starts_with("ok committed index="));
        let refused = handle_command(&pd, "remove-node 2");
        assert!(refused.starts_with(
            "err: membership change refused, nothing applied: removing node 2 would leave keys with no \
             surviving replica in the default zone (ring rf 1)."
        ), "{refused}");
        assert!(refused.contains("--force"), "{refused}");
        assert!(handle_command(&pd, "members").ends_with("\n1 1 -\n2 1 -"));
        let absent = handle_command(&pd, "remove-node 3");
        assert!(!absent.contains("surviving replica"), "a non-member is not checked: {absent}");
        let forced = handle_command(&pd, "remove-node 2 --force");
        assert!(forced.starts_with("ok committed index="), "{forced}");
        assert!(forced.ends_with("surviving-replica and cleanup gates skipped (--force)"), "{forced}");
        assert!(handle_command(&pd, "members").ends_with("\n1 1 -"));
        pd.shutdown();

        let engine =
            Engine::create_prewiped(MemBlockDevice::new(4096, 16 * 1024), Config::default());
        let pd = PdDaemon::spawn_single(
            1,
            engine,
            RingConfig { vnodes: 128, rf: 2 },
            Vec::new(),
        )
        .unwrap();
        pd.bootstrap_members((1..=3).map(|node| (node, 1, Vec::new())).collect());
        let deadline = Instant::now() + Duration::from_secs(5);
        while !handle_command(&pd, "members").ends_with("\n3 1 -") {
            assert!(Instant::now() < deadline, "bootstrap membership not committed");
            std::thread::sleep(Duration::from_millis(10));
        }
        let gated = handle_command(&pd, "remove-node 3");
        assert!(gated.contains("has no data address"), "rf 2 passes on to the cleanup gate: {gated}");
        assert!(!gated.contains("surviving replica"), "{gated}");
        assert!(handle_command(&pd, "create-zone single 1 unsorted").starts_with("ok committed index="));
        let refused = handle_command(&pd, "remove-node 3");
        assert!(refused.contains("removing node 3 would leave keys with no surviving replica in zone 1 \
                                  (single, rf 1)."), "{refused}");
        assert!(handle_command(&pd, "remove-node 3 --force").starts_with("ok committed index="));
        assert!(handle_command(&pd, "members").ends_with("\n2 1 -"));
        pd.shutdown();
    }

    #[test]
    fn create_zone_presplit_commits_k_ranges() {
        let pd = spawn_pd();
        assert!(handle_command(&pd, "create-zone s 1 sorted presplit=4")
            .starts_with("ok committed index="));
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(id) = pd.catalog().by_name("s") {
                if pd.range_map(id).num_ranges() == 4 {
                    assert!(pd.range_map(id).is_canonical());
                    assert_eq!(pd.range_map(id).unrehomed_ranges(), 0);
                    break;
                }
            }
            assert!(Instant::now() < deadline, "presplit did not reach 4 ranges");
            std::thread::sleep(Duration::from_millis(20));
        }
        pd.shutdown();
    }

    #[test]
    fn create_zone_explicit_presplit_commits_boundaries() {
        let pd = spawn_pd();
        assert!(handle_command(&pd, "create-zone s 1 sorted presplit=app/3,app/6,app/9")
            .starts_with("ok committed index="));
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(id) = pd.catalog().by_name("s") {
                let map = pd.range_map(id);
                if map.num_ranges() == 4 {
                    assert_eq!(
                        map.splits(),
                        [b"app/3".to_vec(), b"app/6".to_vec(), b"app/9".to_vec()]
                    );
                    assert!(map.is_canonical());
                    assert_eq!(map.unrehomed_ranges(), 0);
                    break;
                }
            }
            assert!(Instant::now() < deadline, "explicit presplit did not commit");
            std::thread::sleep(Duration::from_millis(20));
        }
        pd.shutdown();
    }

    #[test]
    fn split_zone_canonical_and_mode_validation() {
        let pd = spawn_pd();
        pd.create_zone(ZoneConfig {
            name: "s".into(),
            rf: 1,
            ordering: Ordering::Sorted,
            scope: None,
        }).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let zid = loop {
            if let Some(id) = pd.catalog().by_name("s") {
                break id;
            }
            assert!(Instant::now() < deadline, "zone did not commit");
            std::thread::sleep(Duration::from_millis(20));
        };

        assert!(handle_command(&pd, "split-zone s m canonical").starts_with("ok queued"));
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if pd.range_map(zid).splits() == [b"m".to_vec()] {
                assert!(pd.range_map(zid).is_canonical());
                break;
            }
            assert!(Instant::now() < deadline, "canonical split did not commit");
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(
            handle_command(&pd, "split-zone s t bogus"),
            "err: expected split-zone <zone> <key> [canonical], got mode \"bogus\""
        );
        pd.shutdown();
    }

    #[test]
    fn create_zone_presplit_validation() {
        let pd = spawn_pd();
        assert!(handle_command(&pd, "create-zone u 1 unsorted presplit=4").starts_with("err:"));
        assert!(handle_command(&pd, "create-zone a 1 sorted presplit=1").starts_with("err:"));
        assert!(handle_command(&pd, "create-zone b 1 sorted presplit=500").starts_with("err:"));
        assert!(handle_command(&pd, "create-zone c 1 sorted split=4").starts_with("err:"));
        assert!(handle_command(&pd, "create-zone e 1 weird").starts_with("err:"));

        assert_eq!(
            handle_command(&pd, "create-zone f 1 sorted presplit=b,a"),
            "err: presplit boundary keys must be strictly increasing in byte order"
        );
        assert_eq!(
            handle_command(&pd, "create-zone g 1 sorted presplit=a,a"),
            "err: presplit boundary keys must be strictly increasing in byte order"
        );
        assert_eq!(
            handle_command(&pd, "create-zone h 1 sorted presplit=a,,b"),
            "err: presplit boundary keys must be non-empty"
        );
        let too_many = (0..256)
            .map(|i| format!("k{i:03}"))
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(
            handle_command(
                &pd,
                &format!("create-zone i 1 sorted presplit={too_many}")
            ),
            "err: presplit accepts at most 255 boundary keys (256 ranges)"
        );
        pd.shutdown();
    }
}
