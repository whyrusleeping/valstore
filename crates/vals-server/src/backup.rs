use std::path::{Path, PathBuf};
use std::process::ExitCode;

use vals_cluster::backup::{self, NetCluster, PdStateCapture, RestoreTarget};
use vals_cluster::zone::ZoneConfig;

use crate::config::PdConfig;

pub const USAGE: &str = "valsd backup create -s <pd-admin-socket> --out <new-dir> [--stale-copies fail|omit|keep]\n  \
     valsd backup verify <dir>\n  \
     valsd backup restore-pd -c <target-pd.conf> [--incarnation <32 hex digits>] <dir>\n  \
     valsd backup load -s <target-pd-admin-socket> <dir>\n  \
     valsd backup compare -s <target-pd-admin-socket> <dir>";

pub fn backup_main(args: &[String]) -> ExitCode {
    match args.split_first() {
        Some((cmd, rest)) if cmd == "create" => match parse_flags(rest, &["-s", "--out", "--stale-copies"]) {
            Ok((flags, [])) => match (flags.get("-s"), flags.get("--out")) {
                (Some(socket), Some(out)) => {
                    let stale = flags
                        .get("--stale-copies")
                        .map_or(Ok(backup::StaleCopies::Fail), |s| backup::StaleCopies::parse(s));
                    match stale {
                        Ok(stale) => finish("backup create", create(socket, out, stale)),
                        Err(e) => usage_error(&e),
                    }
                }
                _ => usage_error("backup create requires `-s <pd-admin-socket>` and `--out <new-dir>`"),
            },
            Ok(_) => usage_error("backup create takes no positional arguments"),
            Err(e) => usage_error(&e),
        },
        Some((cmd, rest)) if cmd == "verify" => match rest {
            [dir] => finish("backup verify", verify(Path::new(dir))),
            _ => usage_error("backup verify takes exactly one backup directory"),
        },
        Some((cmd, rest)) if cmd == "restore-pd" => match parse_flags(rest, &["-c", "--incarnation"]) {
            Ok((flags, [dir])) => match flags.get("-c") {
                Some(config) => match flags.get("--incarnation").map(|hex| parse_incarnation(hex)).transpose() {
                    Ok(incarnation) => finish("backup restore-pd", restore_pd(config, incarnation, Path::new(dir))),
                    Err(e) => usage_error(&e),
                },
                None => usage_error("backup restore-pd requires `-c <target-pd.conf>`"),
            },
            Ok(_) => usage_error("backup restore-pd takes exactly one backup directory"),
            Err(e) => usage_error(&e),
        },
        Some((cmd, rest)) if cmd == "load" || cmd == "compare" => match parse_flags(rest, &["-s"]) {
            Ok((flags, [dir])) => match flags.get("-s") {
                Some(socket) if cmd == "load" => finish("backup load", load(socket, Path::new(dir))),
                Some(socket) => finish("backup compare", compare(socket, Path::new(dir))),
                None => usage_error(&format!("backup {cmd} requires `-s <target-pd-admin-socket>`")),
            },
            Ok(_) => usage_error(&format!("backup {cmd} takes exactly one backup directory")),
            Err(e) => usage_error(&e),
        },
        _ => usage_error("expected `backup create|verify|restore-pd|load|compare`"),
    }
}

fn usage_error(message: &str) -> ExitCode {
    eprintln!("valsd backup: {message}\nusage:\n  {USAGE}");
    ExitCode::from(2)
}

fn finish(what: &str, result: Result<String, String>) -> ExitCode {
    match result {
        Ok(summary) => {
            println!("ok {summary}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            println!("err: {what}: {e}");
            ExitCode::FAILURE
        }
    }
}

type Flags = std::collections::BTreeMap<String, String>;

fn parse_flags<'a>(args: &'a [String], known: &[&str]) -> Result<(Flags, &'a [String]), String> {
    let mut flags = Flags::new();
    let mut rest = args;
    while let [flag, tail @ ..] = rest {
        if !flag.starts_with('-') {
            break;
        }
        if !known.contains(&flag.as_str()) {
            return Err(format!("unknown flag {flag:?}"));
        }
        let [value, tail @ ..] = tail else {
            return Err(format!("{flag} requires a value"));
        };
        if flags.insert(flag.clone(), value.clone()).is_some() {
            return Err(format!("duplicate flag {flag}"));
        }
        rest = tail;
    }
    Ok((flags, rest))
}

fn parse_incarnation(hex: &str) -> Result<u128, String> {
    let id = (hex.len() == 32)
        .then(|| u128::from_str_radix(hex, 16).ok())
        .flatten()
        .ok_or_else(|| format!("--incarnation expects 32 hex digits, got {hex:?}"))?;
    if id == 0 {
        return Err("--incarnation must be non-zero".into());
    }
    Ok(id)
}

fn absolute(out: &Path) -> Result<PathBuf, String> {
    let name = out.file_name().ok_or_else(|| format!("{} has no directory name", out.display()))?;
    let parent = match out.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let parent = parent
        .canonicalize()
        .map_err(|e| format!("{}: {e}", parent.display()))?;
    Ok(parent.join(name))
}

fn capture_via(socket: &Path, out: &Path, n: &mut u32) -> Result<PdStateCapture, String> {
    *n += 1;
    let path = out.join(format!(".pd-capture-{n}"));
    let reply = crate::admin::request(socket, &format!("export-state {}", path.display()))
        .map_err(|e| format!("PD admin socket {}: {e}", socket.display()))?;
    if !reply.starts_with("ok") {
        let _ = std::fs::remove_file(&path);
        return Err(format!("PD refused export-state: {reply}"));
    }
    let bytes = std::fs::read(&path).map_err(|e| format!("read {}: {e}", path.display()))?;
    std::fs::remove_file(&path).map_err(|e| format!("remove {}: {e}", path.display()))?;
    PdStateCapture::decode(&bytes)
}

fn with_capture_dir<T>(f: impl FnOnce(&Path) -> Result<T, String>) -> Result<T, String> {
    use std::os::unix::fs::PermissionsExt;
    let dir = std::env::temp_dir().join(format!(
        "valsd-backup-{}-{:x}",
        std::process::id(),
        backup::random_id().map_err(|e| format!("draw a scratch name: {e}"))?
    ));
    std::fs::create_dir(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    let result = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o1733))
        .map_err(|e| format!("chmod {}: {e}", dir.display()))
        .and_then(|()| f(&dir));
    let _ = std::fs::remove_dir_all(&dir);
    result
}

fn create(socket: &str, out: &str, stale: backup::StaleCopies) -> Result<String, String> {
    vals_cluster::observe::init_stderr_logger();
    let out = absolute(Path::new(out))?;
    let mut captures = 0u32;
    let capture_dir = out.clone();
    let socket = PathBuf::from(socket);
    let mut cluster = NetCluster::new(|| capture_via(&socket, &capture_dir, &mut captures));
    let tool = format!("valsd {}", env!("CARGO_PKG_VERSION"));
    let manifest = backup::create_backup(&out, &mut cluster, &tool, stale, &mut |line: &str| {
        eprintln!("valsd backup: {line}")
    })?;
    let (records, puts, tombstones) = manifest.zones.iter().fold((0, 0, 0), |acc, zone| {
        (acc.0 + zone.counts.records, acc.1 + zone.counts.puts, acc.2 + zone.counts.tombstones)
    });
    Ok(format!(
        "backup {:032x} -> {}: source incarnation {:032x}, PD index {}..{}, {} zones, {records} records \
         ({puts} put, {tombstones} tombstone)",
        manifest.backup_id,
        out.display(),
        manifest.source_incarnation,
        manifest.pd_applied,
        manifest.pd_applied_end,
        manifest.zones.len(),
    ))
}

fn verify(dir: &Path) -> Result<String, String> {
    let (manifest, report) = backup::verify(dir)?;
    Ok(format!(
        "verified {}: backup {:032x}, source incarnation {:032x}, {} zones, {} records ({} put, {} tombstone), \
         {} range tombstones, {} bytes",
        dir.display(),
        manifest.backup_id,
        manifest.source_incarnation,
        report.zones,
        report.counts.records,
        report.counts.puts,
        report.counts.tombstones,
        report.range_tombstones,
        report.bytes,
    ))
}

fn restore_pd(config: &str, incarnation: Option<u128>, dir: &Path) -> Result<String, String> {
    let cfg = PdConfig::load(Path::new(config)).map_err(|e| format!("{config}: {e}"))?;
    let (placement, _) = crate::run_pd::resolve_placement(&cfg)?;
    if std::fs::read_dir(&cfg.data_dir).is_ok_and(|mut entries| entries.next().is_some()) {
        return Err(format!(
            "PD data_dir {} is not empty; restore-pd seeds a new PD and never overwrites one",
            cfg.data_dir.display()
        ));
    }
    let voters: Vec<u64> = if cfg.pd_peers.is_empty() {
        vec![cfg.pd_id]
    } else {
        cfg.pd_peers.iter().map(|&(id, _)| id).collect()
    };
    let incarnation = match incarnation {
        Some(id) => id,
        None if voters.len() > 1 => {
            return Err(format!(
                "a {}-voter PD must be seeded identically on every voter: pass the same \
                 `--incarnation <32 hex digits>` to restore-pd on each one (for example {:032x}, drawn now)",
                voters.len(),
                backup::random_id().map_err(|e| format!("draw an incarnation: {e}"))?
            ))
        }
        None => backup::random_id().map_err(|e| format!("draw an incarnation: {e}"))?,
    };
    let target = RestoreTarget {
        raft_dir: cfg.data_dir.join("raft"),
        voters,
        members: cfg.data_nodes.clone(),
        ring: (cfg.vnodes, cfg.rf as usize),
        boot_zones: cfg
            .zones
            .iter()
            .map(|zone| ZoneConfig { name: zone.name.clone(), rf: zone.rf, ordering: zone.ordering, scope: None })
            .collect(),
        opaque_range_maps: placement.opaque_range_maps,
    };
    let restored = backup::restore_pd(dir, &target, incarnation)?;
    write_restore_seed(&cfg.data_dir, &restored)?;
    Ok(format!(
        "restored pd {} into {} from backup {:032x}: incarnation {:032x} (predecessor {:032x}), index {} term {}, \
         {} entries, state digest {:08x}",
        cfg.pd_id,
        target.raft_dir.display(),
        restored.backup_id,
        restored.incarnation,
        restored.lineage.predecessor,
        restored.index,
        restored.term,
        restored.entries,
        restored.digest,
    ))
}

pub const RESTORE_SEED_FILE: &str = "restore-seed";
const RESTORE_SEED_HEADER: &str = "vals-restore-seed 1";

fn write_restore_seed(data_dir: &Path, restored: &backup::RestoredPd) -> Result<(), String> {
    use std::io::Write;
    let text = format!(
        "{RESTORE_SEED_HEADER}\ndigest {:08x}\nbackup_id {:032x}\nincarnation {:032x}\nindex {}\n",
        restored.digest, restored.backup_id, restored.incarnation, restored.index
    );
    let path = data_dir.join(RESTORE_SEED_FILE);
    let staging = data_dir.join(format!(".{RESTORE_SEED_FILE}.tmp"));
    let mut file = std::fs::File::create(&staging).map_err(|e| format!("create {}: {e}", staging.display()))?;
    file.write_all(text.as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|e| format!("write {}: {e}", staging.display()))?;
    std::fs::rename(&staging, &path).map_err(|e| format!("rename {}: {e}", path.display()))?;
    std::fs::File::open(data_dir)
        .and_then(|dir| dir.sync_all())
        .map_err(|e| format!("sync {}: {e}", data_dir.display()))
}

pub fn read_restore_seed(data_dir: &Path) -> Result<Option<u32>, String> {
    let path = data_dir.join(RESTORE_SEED_FILE);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("read {}: {e}", path.display())),
    };
    let mut lines = text.lines();
    let digest = (lines.next() == Some(RESTORE_SEED_HEADER))
        .then(|| lines.find_map(|line| line.strip_prefix("digest ")))
        .flatten()
        .and_then(|hex| u32::from_str_radix(hex, 16).ok())
        .ok_or_else(|| format!("{} is not a restore seed record written by restore-pd", path.display()))?;
    Ok(Some(digest))
}

fn load(socket: &str, dir: &Path) -> Result<String, String> {
    vals_cluster::observe::init_stderr_logger();
    let socket = PathBuf::from(socket);
    let report = with_capture_dir(|scratch| {
        let mut captures = 0u32;
        backup::load_backup(dir, &mut || capture_via(&socket, scratch, &mut captures), &mut |line: &str| {
            eprintln!("valsd backup: {line}")
        })
    })?;
    Ok(format!(
        "loaded {}: {} zones, {} records ({} put, {} tombstone) and {} range tombstones on every replica",
        dir.display(),
        report.zones,
        report.counts.records,
        report.counts.puts,
        report.counts.tombstones,
        report.range_tombstones,
    ))
}

fn compare(socket: &str, dir: &Path) -> Result<String, String> {
    vals_cluster::observe::init_stderr_logger();
    let socket = PathBuf::from(socket);
    let report = with_capture_dir(|scratch| {
        let mut captures = 0u32;
        let mut cluster = NetCluster::new(|| capture_via(&socket, scratch, &mut captures));
        backup::compare_backup(dir, &mut cluster, &mut |line: &str| eprintln!("valsd backup: {line}"))
    })?;
    let matched: u64 = report.zones.iter().map(|zone| zone.matched).sum();
    let matched_ranges: u64 = report.zones.iter().map(|zone| zone.matched_range_tombstones).sum();
    let collected: u64 = report.zones.iter().map(|zone| zone.collected_tombstones).sum();
    let differences = report.differences();
    if differences > 0 {
        let mut message = format!(
            "{differences} differences between the target and {} ({matched} records match)",
            dir.display()
        );
        for sample in &report.samples {
            message.push_str("\n  ");
            message.push_str(sample);
        }
        return Err(message);
    }
    Ok(format!(
        "target matches {}: {} zones, {matched} records and {matched_ranges} range tombstones equal, \
         {collected} backup tombstones already collected",
        dir.display(),
        report.zones.len(),
    ))
}
