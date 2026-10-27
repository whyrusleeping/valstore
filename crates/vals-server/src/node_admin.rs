use std::path::PathBuf;
use vals_cluster::node::NodeDumper;

use crate::admin::AdminReply;

const DUMP_USAGE: &str =
    "usage: dump <zone-id-or-name> <out-path> [skip-tombstones] [parallel=N] [rate_mib=N] [shard=S]";

pub fn dispatch(dumper: &NodeDumper, line: &str, reply_tx: AdminReply) {
    let parts: Vec<&str> = line.split_whitespace().collect();
    match parts.as_slice() {
        ["ping"] => {
            let _ = reply_tx.send("ok".into());
        }
        ["zone-stats"] => {
            let reply = match dumper.all_zone_stats() {
                Ok(stats) => {
                    let mut out = String::from(
                        "ok shard,zone_id,used_segments,free_segments,live_keys,resident_runs",
                    );
                    for (shard, stats) in stats {
                        out.push_str(&format!(
                            "\n{shard},{},{},{},{},{}",
                            stats.zone_id,
                            stats.used_segments,
                            stats.free_segments,
                            stats.live_keys,
                            stats.resident_runs,
                        ));
                    }
                    out
                }
                Err(e) => format!("err: {e}"),
            };
            let _ = reply_tx.send(reply);
        }
        ["dump", zone, out, opts @ ..] => {
            let mut skip = false;
            let mut parallel = 1usize;
            let mut rate_mib = None;
            let mut shard = 0usize;
            for opt in opts {
                if *opt == "skip-tombstones" {
                    skip = true;
                } else if let Some(n) = opt.strip_prefix("parallel=") {
                    match n.parse::<usize>() {
                        Ok(v) if v >= 1 => parallel = v.min(64),
                        _ => {
                            let _ = reply_tx.send(format!("err: bad parallel={n:?} (want >= 1)"));
                            return;
                        }
                    }
                } else if let Some(n) = opt.strip_prefix("rate_mib=") {
                    match n.parse::<u64>() {
                        Ok(v) if v > 0 => rate_mib = Some(v),
                        _ => {
                            let _ = reply_tx.send(format!(
                                "err: bad rate_mib={n:?} (want positive integer)"
                            ));
                            return;
                        }
                    }
                } else if let Some(s) = opt.strip_prefix("shard=") {
                    match s.parse::<usize>() {
                        Ok(v) => shard = v,
                        _ => {
                            let _ = reply_tx.send(format!("err: bad shard={s:?}"));
                            return;
                        }
                    }
                } else {
                    let _ = reply_tx.send(format!(
                        "err: unknown dump option {opt:?} \
                         (skip-tombstones|parallel=N|rate_mib=N|shard=S)"
                    ));
                    return;
                }
            }
            let zone = zone.to_string();
            let out = PathBuf::from(out);
            let dumper = dumper.clone();
            let reply_thread = reply_tx.clone();
            let spawned = std::thread::Builder::new()
                .name("vals-admin-dump".into())
                .spawn(move || {
                    let reply = match dumper.dump_zone(
                        shard, zone, out, skip, parallel, rate_mib,
                    ) {
                        Ok(summary) => format!("ok {summary}"),
                        Err(e) => format!("err: {e}"),
                    };
                    let _ = reply_thread.send(reply);
                });
            if let Err(e) = spawned {
                let _ = reply_tx.send(format!("err: spawn dump thread: {e}"));
            }
        }
        ["dump", ..] => {
            let _ = reply_tx.send(format!("err: {DUMP_USAGE}"));
        }
        [] => {
            let _ = reply_tx.send("err: empty command".into());
        }
        [cmd, ..] => {
            let _ = reply_tx.send(format!(
                "err: unknown command {cmd:?} (ping|zone-stats|dump)"
            ));
        }
    }
}
