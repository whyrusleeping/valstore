use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MembershipChange {
    Add(NodeId),
    Remove(NodeId),
}

const GATE_ATTEMPTS: usize = 2;
const GATE_BUDGET: Duration = Duration::from_secs(4);
const GATE_POLL: Duration = Duration::from_millis(200);

fn ask(addr: SocketAddr, req: &Request) -> Result<Response, String> {
    let mut last = String::new();
    for attempt in 0..GATE_ATTEMPTS {
        match blocking_request(addr, req) {
            Ok(Response::Busy) => last = "busy".into(),
            Ok(Response::Err(e)) => return Err(e),
            Ok(response) => return Ok(response),
            Err(e) => last = e.to_string(),
        }
        if attempt + 1 < GATE_ATTEMPTS {
            std::thread::sleep(GATE_POLL);
        }
    }
    Err(last)
}

fn show_key(key: &[u8]) -> String {
    match std::str::from_utf8(key) {
        Ok(text) if text.chars().all(|c| !c.is_control()) => format!("{text:?}"),
        _ => format!("hex={}", key.iter().map(|b| format!("{b:02x}")).collect::<String>()),
    }
}

#[derive(Default)]
struct Survey {
    problems: Vec<String>,
    unreachable: Vec<String>,
    answered: usize,
    gc_unknown_or_on: bool,
    settling: bool,
}

type Ask<'a> = &'a mut dyn FnMut(SocketAddr, &Request) -> Result<Response, String>;

fn survey(members: &AddressedMembers, change: MembershipChange, expected: &[(NodeId, u32)], ask: Ask<'_>) -> Survey {
    let mut out = Survey::default();
    for (node, _, addrs) in members {
        let departing = change == MembershipChange::Remove(*node);
        if addrs.is_empty() {
            out.gc_unknown_or_on = true;
            if !departing {
                out.unreachable.push(format!("node {node} has no data address in the PD state"));
            }
            continue;
        }
        for (shard, &addr) in addrs.iter().enumerate() {
            let who = format!("node {node} shard {shard} ({addr})");
            let (ring_epoch, gc_grace_ms, unsettled) = match ask(addr, &Request::CleanupStatus) {
                Ok(Response::CleanupStatus { ring_epoch, gc_grace_ms, unsettled, .. }) => {
                    (ring_epoch, gc_grace_ms, unsettled)
                }
                Ok(other) => {
                    out.gc_unknown_or_on = true;
                    if !departing {
                        out.unreachable.push(format!("{who}: expected a cleanup-status reply, got {other:?}"));
                    }
                    continue;
                }
                Err(e) => {
                    out.gc_unknown_or_on = true;
                    if !departing {
                        out.unreachable.push(format!(
                            "{who} did not answer: {e} (a node that predates the cleanup-status request \
                             must be upgraded)"
                        ));
                    }
                    continue;
                }
            };
            out.gc_unknown_or_on |= gc_grace_ms > 0;
            if departing {
                continue;
            }
            out.answered += 1;
            match ask(addr, &Request::Members) {
                Ok(Response::Members { epoch, members, .. }) => {
                    let mut adopted = members;
                    adopted.sort_unstable();
                    if adopted != expected || epoch != ring_epoch {
                        out.settling = true;
                        out.problems.push(format!(
                            "{who} has not adopted the current membership yet (ring epoch {epoch})"
                        ));
                        continue;
                    }
                }
                Ok(other) => {
                    out.unreachable.push(format!("{who}: expected a members reply, got {other:?}"));
                    continue;
                }
                Err(e) => {
                    out.unreachable.push(format!("{who}: {e}"));
                    continue;
                }
            }
            for zone in unsettled {
                out.settling |= !zone.lapped && !zone.pending_rehome;
                out.problems.push(if zone.pending_rehome {
                    format!(
                        "{who} zone {}: a re-home is pending; its target may hold copies no owner has \
                         confirmed, and a membership change would bypass the copy's checks",
                        zone.zone_id
                    )
                } else if zone.kept > 0 {
                    let samples: Vec<String> = zone.samples.iter().map(|k| show_key(k)).collect();
                    format!(
                        "{who} zone {}: cleanup kept {} copies of keys the node does not own that not every \
                         owner confirmed (an owner refuses a put older than its tombstone-GC floor, so the \
                         delete may have been collected; an unreachable owner cannot confirm either), e.g. {}",
                        zone.zone_id,
                        zone.kept,
                        samples.join(", ")
                    )
                } else {
                    format!(
                        "{who} zone {}: cleanup has not finished a lap since this check started (ring epoch \
                         {ring_epoch})",
                        zone.zone_id
                    )
                });
            }
        }
    }
    out
}

pub fn check_membership_change(
    members: &AddressedMembers,
    range_maps: &HashMap<ZoneId, RangeMap>,
    change: MembershipChange,
    incoming: &[SocketAddr],
) -> Result<String, String> {
    check_with(members, range_maps, change, incoming, &mut ask)
}

fn incoming_problems(node: NodeId, incoming: &[SocketAddr], ask: Ask<'_>) -> Vec<String> {
    if incoming.is_empty() {
        return vec![format!(
            "incoming node {node}: no data address was given, so the PD cannot check that it holds no data"
        )];
    }
    let mut problems = Vec::new();
    for (shard, &addr) in incoming.iter().enumerate() {
        let who = format!("incoming node {node} shard {shard} ({addr})");
        match ask(addr, &Request::CleanupStatus) {
            Ok(Response::CleanupStatus { records: 0, .. }) => {}
            Ok(Response::CleanupStatus { records, .. }) => problems.push(format!(
                "{who} holds {records} records; a node that joins must hold no data (wipe its data first), \
                 because it would serve copies whose deletes the members may have collected"
            )),
            Ok(other) => problems.push(format!("{who}: expected a cleanup-status reply, got {other:?}")),
            Err(e) => problems.push(format!("{who} did not answer: {e}")),
        }
    }
    problems
}

fn check_with(
    members: &AddressedMembers,
    range_maps: &HashMap<ZoneId, RangeMap>,
    change: MembershipChange,
    incoming: &[SocketAddr],
    ask: Ask<'_>,
) -> Result<String, String> {
    let mut expected: Vec<(NodeId, u32)> = members.iter().map(|(n, w, _)| (*n, *w)).collect();
    expected.sort_unstable();
    let started = std::time::Instant::now();
    let mut s = loop {
        let s = survey(members, change, &expected, &mut *ask);
        if !s.gc_unknown_or_on || !s.settling || started.elapsed() + GATE_POLL >= GATE_BUDGET {
            break s;
        }
        std::thread::sleep(GATE_POLL);
    };
    if !s.gc_unknown_or_on {
        return Ok("tombstone GC is off on every node".into());
    }
    let mut pending: Vec<ZoneId> = range_maps
        .iter()
        .filter(|(_, map)| map.pending().iter().any(Option::is_some))
        .map(|(zone_id, _)| *zone_id)
        .collect();
    pending.sort_unstable();
    for zone_id in pending {
        s.problems.push(format!(
            "zone {zone_id} has a pending re-home in the PD state; wait for it to finish or abort"
        ));
    }
    if let MembershipChange::Add(node) = change {
        if !members.iter().any(|(n, _, _)| *n == node) {
            s.problems.extend(incoming_problems(node, incoming, ask));
        }
    }
    s.problems.append(&mut s.unreachable);
    if s.problems.is_empty() {
        return Ok(format!("cleanup settled on {} node shards", s.answered));
    }
    Err(s.problems.join("; "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::UnsettledZone;
    use MembershipChange::{Add, Remove};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn members() -> AddressedMembers {
        vec![(1, 1, vec![addr(1)]), (2, 1, vec![addr(2)]), (3, 1, vec![addr(3)])]
    }

    #[derive(Clone, Copy, Default)]
    struct Fake {
        gc: [u64; 3],
        kept_on_3: u64,
        pending_on_3: bool,
        down: &'static [u16],
        records_on_4: u64,
    }

    impl Fake {
        fn gc(gc: [u64; 3]) -> Self {
            Fake { gc, ..Fake::default() }
        }

        fn ask(self) -> impl FnMut(SocketAddr, &Request) -> Result<Response, String> {
            move |a, req| {
                if self.down.contains(&a.port()) {
                    return Err("connection refused".into());
                }
                let on_3 = a.port() == 3 && (self.kept_on_3 > 0 || self.pending_on_3);
                Ok(match req {
                    Request::CleanupStatus => Response::CleanupStatus {
                        ring_epoch: 4,
                        gc_grace_ms: self.gc.get(a.port() as usize - 1).copied().unwrap_or(60_000),
                        records: if a.port() == 4 { self.records_on_4 } else { 10 },
                        unsettled: on_3
                            .then(|| UnsettledZone {
                                zone_id: 0,
                                lapped: true,
                                pending_rehome: self.pending_on_3,
                                kept: self.kept_on_3,
                                samples: vec![b"k".to_vec()],
                            })
                            .into_iter()
                            .collect(),
                    },
                    Request::Members => Response::Members {
                        epoch: 4,
                        vnodes: 64,
                        rf: 1,
                        members: vec![(1, 1), (2, 1), (3, 1)],
                        addrs: Vec::new(),
                    },
                    other => panic!("unexpected {other:?}"),
                })
            }
        }

        fn check(self, change: MembershipChange) -> Result<String, String> {
            check_with(&members(), &HashMap::new(), change, &[addr(4)], &mut self.ask())
        }
    }

    #[test]
    fn the_gate_fails_closed_on_unknown_or_mixed_gc_and_unreachable_members() {
        let on = [60_000; 3];
        let kept = |gc, down| Fake { kept_on_3: 1, down, ..Fake::gc(gc) };
        assert!(kept([0, 0, 0], &[]).check(Add(4)).is_ok(), "GC off everywhere: no delete was collected");
        assert!(kept([0, 0, 0], &[]).check(Remove(1)).is_ok());
        assert!(Fake::gc(on).check(Remove(1)).is_ok(), "settled");
        let refused = kept(on, &[]).check(Remove(1)).unwrap_err();
        assert!(refused.contains("node 3 shard 0") && refused.contains("kept 1 copies"), "{refused}");
        assert!(refused.contains("\"k\""), "{refused}");
        assert!(kept([60_000, 0, 0], &[]).check(Remove(1)).is_err(), "the departing owner collects tombstones");
        assert!(kept([0, 0, 0], &[1]).check(Remove(1)).is_err(), "the departing owner's GC is unknown");
        let settled_rest = Fake { down: &[1], ..Fake::gc([0, 0, 0]) };
        assert!(settled_rest.check(Remove(1)).is_ok(), "an unreachable departing node needs a settled rest");
        let down = Fake { down: &[2], ..Fake::gc([0, 0, 0]) }.check(Add(4)).unwrap_err();
        assert!(down.contains("node 2 shard 0") && down.contains("did not answer"), "{down}");
        assert!(Fake { down: &[2], ..Fake::gc(on) }.check(Remove(1)).is_err());
        let mut addressless = members();
        addressless[1].2.clear();
        let none = check_with(&addressless, &HashMap::new(), Add(4), &[addr(4)], &mut Fake::default().ask());
        assert!(none.unwrap_err().contains("node 2 has no data address"));
    }

    #[test]
    fn a_node_that_joins_while_tombstone_gc_is_on_must_hold_no_data() {
        let on = [60_000; 3];
        let add = |fake: Fake, incoming: &[SocketAddr]| {
            check_with(&members(), &HashMap::new(), Add(4), incoming, &mut fake.ask())
        };
        assert!(add(Fake::gc(on), &[addr(4)]).is_ok(), "an empty node joins");
        let full = add(Fake { records_on_4: 7, ..Fake::gc(on) }, &[addr(4)]).unwrap_err();
        assert!(full.contains("incoming node 4 shard 0") && full.contains("holds 7 records"), "{full}");
        let down = add(Fake { down: &[4], ..Fake::gc(on) }, &[addr(4)]).unwrap_err();
        assert!(down.contains("incoming node 4 shard 0"), "{down}");
        assert!(add(Fake::gc(on), &[]).unwrap_err().contains("no data address was given"));
        assert!(add(Fake { records_on_4: 7, ..Fake::gc([0, 0, 0]) }, &[addr(4)]).is_ok());
        let weight_change =
            check_with(&members(), &HashMap::new(), Add(3), &[], &mut Fake { records_on_4: 7, ..Fake::gc(on) }.ask());
        assert!(weight_change.is_ok(), "a weight change of a member is checked as a member");
    }

    #[test]
    fn a_pending_rehome_blocks_a_membership_change() {
        let on = [60_000; 3];
        let on_node = Fake { pending_on_3: true, ..Fake::gc(on) }.check(Remove(1)).unwrap_err();
        assert!(on_node.contains("node 3 shard 0 (127.0.0.1:3) zone 0: a re-home is pending"), "{on_node}");
        let split = b"m".to_vec();
        let pending = RangeMap::from_splits_tokens_and_pending(
            vec![split.clone()],
            vec![Vec::new(), Vec::new()],
            vec![None, Some(split)],
        )
        .unwrap();
        let maps = HashMap::from([(7, pending)]);
        let in_pd = check_with(&members(), &maps, Remove(1), &[], &mut Fake::gc(on).ask()).unwrap_err();
        assert!(in_pd.contains("zone 7 has a pending re-home in the PD state"), "{in_pd}");
        assert!(check_with(&members(), &maps, Remove(1), &[], &mut Fake::gc([0, 0, 0]).ask()).is_ok());
    }
}
