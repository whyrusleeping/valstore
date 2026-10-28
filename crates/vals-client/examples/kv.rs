//! `kv`: a small command-line client for a running vals cluster.
//!
//! ```text
//! cargo run -p vals-client --example kv -- [OPTIONS] COMMAND
//!
//! Commands:
//!   put KEY VALUE      write VALUE under KEY
//!   get KEY            read KEY; prints the value and its version, or "not found"
//!   delete KEY         delete KEY
//!   scan [FROM [TO]]   print `key<TAB>value` for each key in [FROM, TO) of a
//!                      sorted zone, in key order, then the number of keys
//!
//! Options:
//!   --nodes ID@HOST:PORT,...  seed data nodes
//!                             (default 1@127.0.0.1:7001,2@127.0.0.1:7002,3@127.0.0.1:7003)
//!   --zone NAME               zone to use (default: the `default` zone)
//!   --persistence MODE        `buffered` (default) or `device`: what each
//!                             replica's acknowledgement of a put or delete means
//!   --limit N                 most keys a scan prints (default 100)
//! ```
//!
//! The defaults match the local cluster from the README quick start, so
//! against that cluster:
//!
//! ```text
//! cargo run -p vals-client --example kv -- put hello world
//! cargo run -p vals-client --example kv -- --persistence device put hello world
//! cargo run -p vals-client --example kv -- get hello
//! cargo run -p vals-client --example kv -- --zone events put 2026-06-11/42 click
//! cargo run -p vals-client --example kv -- --zone events scan 2026-06-01/ 2026-07-01/
//! ```
//!
//! The client asks the first seed node that answers for the cluster's current
//! membership and zone catalog, so one live seed is enough; a node with
//! several shard listeners can be given once per address (`1@a,1@b`). Writes
//! and deletes wait for a majority of the key's replicas to acknowledge, and
//! reads and scans ask a majority ([`ReadConcern::Majority`]), so a read sees
//! every acknowledged write, or one with a greater version, whatever the
//! zone's replication factor. By default a replica acknowledges once the
//! write is in its memory; `--persistence device` makes each acknowledgement
//! wait for the replica's device sync, the policy that "Choosing a policy" in
//! the `vals_client` crate documentation recommends for data you cannot
//! afford to lose. Versions print as the writer's hybrid logical clock
//! (`milliseconds.counter`) followed by its writer id.
//!
//! Data operations carry no credentials: the cluster secret only gates control
//! operations, so this client does not need it. The exit status is 0 on
//! success (including "not found"), 1 if the operation failed, and 2 on a
//! usage error. A failed put or delete may still have been applied; the
//! `vals_client` crate documentation describes that and the rest of the
//! client contract.

use std::net::{SocketAddr, ToSocketAddrs};
use std::process::ExitCode;

use vals_client::{
    reactor, Coordinator, NetTransport, NodeId, Persistence, ReadConcern, Ring, Version,
    WriteConcern, WriteOptions, ZoneId, DEFAULT_ZONE,
};

const DEFAULT_NODES: &str = "1@127.0.0.1:7001,2@127.0.0.1:7002,3@127.0.0.1:7003";

const USAGE: &str = "usage: kv [--nodes ID@HOST:PORT,...] [--zone NAME] [--persistence MODE] [--limit N] COMMAND
commands:
  put KEY VALUE
  get KEY
  delete KEY
  scan [FROM [TO]]   (sorted zones only)
options:
  --nodes        seed data nodes (default 1@127.0.0.1:7001,2@127.0.0.1:7002,3@127.0.0.1:7003)
  --zone         zone name (default: the `default` zone)
  --persistence  buffered (default) or device: what a replica's put/delete ack means
  --limit        most keys a scan prints (default 100)";

enum Command {
    Put(String, String),
    Get(String),
    Delete(String),
    Scan(Option<String>, Option<String>),
}

struct Args {
    nodes: Vec<(NodeId, Vec<SocketAddr>)>,
    zone: Option<String>,
    persistence: Persistence,
    limit: usize,
    command: Command,
}

fn parse_nodes(list: &str) -> Result<Vec<(NodeId, Vec<SocketAddr>)>, String> {
    let mut nodes: Vec<(NodeId, Vec<SocketAddr>)> = Vec::new();
    for item in list.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (id, addr) = item
            .split_once('@')
            .ok_or_else(|| format!("bad node `{item}`: expected ID@HOST:PORT"))?;
        let id: NodeId = id
            .trim()
            .parse()
            .map_err(|_| format!("bad node id in `{item}`"))?;
        let addr = addr
            .trim()
            .to_socket_addrs()
            .map_err(|e| format!("bad address in `{item}`: {e}"))?
            .next()
            .ok_or_else(|| format!("address in `{item}` did not resolve"))?;
        match nodes.iter_mut().find(|(n, _)| *n == id) {
            Some((_, addrs)) => addrs.push(addr),
            None => nodes.push((id, vec![addr])),
        }
    }
    if nodes.is_empty() {
        return Err("--nodes lists no nodes".into());
    }
    Ok(nodes)
}

fn parse_args(mut argv: impl Iterator<Item = String>) -> Result<Option<Args>, String> {
    let mut nodes = None;
    let mut zone = None;
    let mut persistence = Persistence::Buffered;
    let mut limit = 100;
    let mut positional = Vec::new();
    while let Some(arg) = argv.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(None),
            "--nodes" | "--zone" | "--persistence" | "--limit" => {
                let value = argv
                    .next()
                    .ok_or_else(|| format!("{arg} needs a value"))?;
                match arg.as_str() {
                    "--nodes" => nodes = Some(parse_nodes(&value)?),
                    "--zone" => zone = Some(value),
                    "--persistence" => {
                        persistence = match value.as_str() {
                            "buffered" => Persistence::Buffered,
                            "device" => Persistence::Device,
                            _ => {
                                return Err(format!(
                                    "--persistence needs `buffered` or `device`, got `{value}`"
                                ))
                            }
                        }
                    }
                    _ => {
                        limit = value
                            .parse()
                            .map_err(|_| format!("--limit needs a number, got `{value}`"))?
                    }
                }
            }
            "--" => positional.extend(argv.by_ref()),
            flag if flag.starts_with("--") => return Err(format!("unknown option `{flag}`")),
            _ => positional.push(arg),
        }
    }
    let command = match positional.as_slice() {
        [cmd, key, value] if cmd == "put" => Command::Put(key.clone(), value.clone()),
        [cmd, key] if cmd == "get" => Command::Get(key.clone()),
        [cmd, key] if cmd == "delete" => Command::Delete(key.clone()),
        [cmd, range @ ..] if cmd == "scan" && range.len() <= 2 => {
            Command::Scan(range.first().cloned(), range.get(1).cloned())
        }
        [] => return Err("missing command".into()),
        _ => return Err(format!("unrecognised command `{}`", positional.join(" "))),
    };
    let nodes = match nodes {
        Some(nodes) => nodes,
        None => parse_nodes(DEFAULT_NODES)?,
    };
    Ok(Some(Args {
        nodes,
        zone,
        persistence,
        limit,
        command,
    }))
}

fn show_version(v: Version) -> String {
    format!("{}.{} writer {:016x}", v.physical_ms(), v.counter(), v.node)
}

fn text(bytes: &[u8]) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(bytes)
}

async fn load_cluster_view(coord: &Coordinator<NetTransport>, args: &Args) -> Result<(), String> {
    let mut last_error = String::new();
    for &(node, _) in &args.nodes {
        match coord.refresh_ring(node).await {
            Ok(members) if members.is_empty() => {
                last_error = format!("node {node} has no membership yet (is the PD running?)")
            }
            Ok(_) => {
                return coord
                    .refresh_catalog(node)
                    .await
                    .map_err(|e| format!("loading the zone catalog from node {node}: {e}"));
            }
            Err(e) => last_error = format!("node {node}: {e}"),
        }
    }
    Err(format!("no seed node answered; last error: {last_error}"))
}

fn resolve_zone(coord: &Coordinator<NetTransport>, name: Option<&str>) -> Result<ZoneId, String> {
    let catalog = coord.catalog();
    match name {
        None => Ok(DEFAULT_ZONE),
        Some(name) => catalog.by_name(name).ok_or_else(|| {
            let known: Vec<&str> = catalog.iter().map(|(_, cfg)| cfg.name.as_str()).collect();
            format!("no zone named `{name}` (known zones: {})", known.join(", "))
        }),
    }
}

async fn run(args: &Args) -> Result<(), String> {
    let transport = NetTransport::connect_sharded(args.nodes.clone());
    let mut ring = Ring::new(3);
    ring.set_members_weighted(args.nodes.iter().map(|&(node, _)| (node, 1)));
    let coord = Coordinator::new(100, ring, ReadConcern::Majority, transport);

    load_cluster_view(&coord, args).await?;
    let zone = resolve_zone(&coord, args.zone.as_deref())?;
    let options = WriteOptions {
        concern: WriteConcern::Majority,
        persistence: args.persistence,
    };

    match &args.command {
        Command::Put(key, value) => {
            let v = coord
                .write_in(zone, key.as_bytes(), value.as_bytes(), options)
                .await
                .map_err(|e| format!("put failed: {e}"))?;
            println!("ok, version {}", show_version(v));
        }
        Command::Get(key) => {
            match coord
                .read_in(zone, key.as_bytes())
                .await
                .map_err(|e| format!("get failed: {e}"))?
            {
                Some(stored) => {
                    println!("{}", text(stored.value.as_deref().unwrap_or_default()));
                    println!("version {}", show_version(stored.version));
                }
                None => println!("not found"),
            }
        }
        Command::Delete(key) => {
            let v = coord
                .delete_in(zone, key.as_bytes(), options)
                .await
                .map_err(|e| format!("delete failed: {e}"))?;
            println!("ok, deleted at version {}", show_version(v));
        }
        Command::Scan(from, to) => {
            let rows = coord
                .scan_in(
                    zone,
                    from.as_deref().map(str::as_bytes),
                    to.as_deref().map(str::as_bytes),
                    ReadConcern::Majority,
                    Some(args.limit),
                )
                .await
                .map_err(|e| format!("scan failed: {e}"))?;
            for (key, value) in &rows {
                println!("{}\t{}", text(key), text(value));
            }
            let noun = if rows.len() == 1 { "key" } else { "keys" };
            println!("({} {noun})", rows.len());
        }
    }
    Ok(())
}

fn main() -> ExitCode {
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(Some(args)) => args,
        Ok(None) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("kv: {e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    match vals_rt::block_on(run(&args), reactor::poll_io) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("kv: {e}");
            ExitCode::FAILURE
        }
    }
}
