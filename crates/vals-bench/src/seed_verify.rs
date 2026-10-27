use std::collections::BTreeSet;
use std::net::SocketAddr;

use vals_cluster::antientropy::{AePeer, DEFAULT_BUCKETS};
use vals_cluster::coord::Transport;
use vals_cluster::{NetTransport, zone::DEFAULT_ZONE};

pub(super) fn workload_seed_verify(peers_arg: &str, zone_id: u32) -> Result<(), String> {
    if zone_id != DEFAULT_ZONE {
        return Err("this check covers only the unsorted default zone".into());
    }
    let nodes: Vec<(u32, Vec<SocketAddr>)> = peers_arg.split(';').map(|peer| {
        let (id, addr) = peer.split_once('@').ok_or("expected id@host:port")?;
        Ok((id.parse::<u32>().map_err(|e| e.to_string())?,
            vec![addr.parse::<SocketAddr>().map_err(|e| e.to_string())?]))
    }).collect::<Result<_, String>>()?;
    if nodes.len() != 3 || nodes.iter().map(|(id, _)| *id).collect::<BTreeSet<_>>() != BTreeSet::from([1, 2, 3]) {
        return Err("expected exactly nodes 1, 2, 3".into());
    }
    let transport = NetTransport::connect_sharded(nodes);
    let poll = vals_cluster::reactor::poll_io;
    let view = vals_rt::block_on(transport.members(1), poll).map_err(|e| e.to_string())?;
    if view.rf != 2 || view.members.len() != 3 || view.members.iter().map(|(id, _)| *id).collect::<BTreeSet<_>>() != BTreeSet::from([1, 2, 3]) {
        return Err(format!("unexpected member view: {view:?}"));
    }
    for node in [2, 3] {
        if vals_rt::block_on(transport.members(node), poll).map_err(|e| e.to_string())? != view {
            return Err(format!("node {node} has a different membership"));
        }
    }
    let mut nonzero = false;
    for peer in [1, 2] {
        let local = vals_rt::block_on(transport.peer(3).tree(zone_id, DEFAULT_BUCKETS, 1, &[0], Some(peer)), poll)
            .map_err(|e| format!("node 3 tree for peer {peer}: {e}"))?;
        let remote = vals_rt::block_on(transport.peer(peer).tree(zone_id, DEFAULT_BUCKETS, 1, &[0], Some(3)), poll)
            .map_err(|e| format!("node {peer} tree for node 3: {e}"))?;
        if local.len() != DEFAULT_BUCKETS as usize || remote.len() != local.len() || local != remote {
            return Err(format!("node 3 and node {peer} differ in the {DEFAULT_BUCKETS} scoped bucket leaves"));
        }
        nonzero |= local.iter().any(|(_, digest)| *digest != 0);
    }
    if !nonzero {
        let counts = [1, 2, 3].map(|node| {
            vals_rt::block_on(transport.peer(node).tree(zone_id, DEFAULT_BUCKETS, 1, &[0], None), poll)
                .map(|leaves| leaves.iter().filter(|(_, digest)| *digest != 0).count())
        });
        return Err(format!("all owned group digests empty; unscoped nonzero leaves: {counts:?}"));
    }
    for node in [1, 2, 3] {
        if vals_rt::block_on(transport.members(node), poll).map_err(|e| e.to_string())? != view {
            return Err("membership changed during digest comparison".into());
        }
    }
    println!("SEED_DIGEST_CHECK status=pass peer_pairs=2 leaves_per_pair={DEFAULT_BUCKETS} membership_epoch={}", view.epoch);
    Ok(())
}
