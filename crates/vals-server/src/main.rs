//! `valsd`: the vals server daemon.
//!
//! One binary runs a data node (`valsd node`), a placement driver
//! (`valsd pd`), or an administrative command (`valsd admin`, `backup`,
//! `dump`, `provision`, and others). Each daemon reads a `key = value` config
//! file and shuts down cleanly on SIGTERM or SIGINT. The operator guide is
//! `docs/deploy.md`.


mod admin;
mod backup;
#[cfg(test)]
mod fail_fast_tests;
#[cfg(all(target_os = "linux", target_env = "gnu"))]
mod allocator;
mod config;
mod dump;
mod keyscan;
mod metrics;
mod node_admin;
mod provision;
mod reenrol;
mod run_node;
mod run_pd;
#[cfg(any(feature = "spdk", test))]
mod spdk_lock;

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use config::{NodeConfig, PdConfig};
use vals_cluster::integrity;

static SHUTDOWN: AtomicBool = AtomicBool::new(false);
static RELOAD: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(sig: libc::c_int) {
    if sig == libc::SIGHUP {
        RELOAD.store(true, Ordering::SeqCst);
    } else {
        SHUTDOWN.store(true, Ordering::SeqCst);
    }
}

fn install_signal_handlers() {
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = on_signal as *const () as usize;
        libc::sigemptyset(&mut sa.sa_mask);
        for sig in [libc::SIGTERM, libc::SIGINT, libc::SIGHUP] {
            libc::sigaction(sig, &sa, std::ptr::null_mut());
        }
    }
}

fn usage() -> ExitCode {
    eprintln!(
        "usage:\n  valsd node -c <config> [--check-config]\n  valsd provision -c <config> \
         --device <pci>=<serial>... --confirm-destroy node-<id>\n  valsd pd -c <config> \
         [--check-config]\n  valsd admin -s <socket> <command...>\n  {}\n  {}\n  {}\n  {}\n\nsee docs/deploy.md",
        dump::USAGE, keyscan::USAGE, reenrol::USAGE, backup::USAGE
    );
    ExitCode::from(2)
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.split_first() {
        Some((cmd, rest)) if cmd == "node" => match daemon_args(rest) {
            Some((path, true)) => check_node_config(path),
            Some((path, false)) => node_main(path),
            None => usage(),
        },
        Some((cmd, rest)) if cmd == "provision" => provision::provision_main(rest),
        Some((cmd, rest)) if cmd == "reenrol" => reenrol::reenrol_main(rest),
        Some((cmd, rest)) if cmd == "backup" => backup::backup_main(rest),
        Some((cmd, rest)) if cmd == "pd" => match daemon_args(rest) {
            Some((path, true)) => check_pd_config(path),
            Some((path, false)) => pd_main(path),
            None => usage(),
        },
        Some((cmd, rest)) if cmd == "admin" => match flag_value(rest, "-s") {
            Some(sock) if rest.len() > 2 => admin_main(sock, &rest[2..]),
            _ => usage(),
        },
        Some((cmd, rest)) if cmd == "keyscan" => keyscan::keyscan_main(rest),
        Some((cmd, rest)) if cmd == "dump" => {
            vals_cluster::observe::init_stderr_logger();
            dump::dump_main(rest)
        }
        _ => usage(),
    }
}

fn flag_value(args: &[String], flag: &str) -> Option<PathBuf> {
    match args {
        [f, v, ..] if f == flag => Some(PathBuf::from(v)),
        _ => None,
    }
}

fn daemon_args(args: &[String]) -> Option<(PathBuf, bool)> {
    match args {
        [config, path] if config == "-c" => Some((PathBuf::from(path), false)),
        [check, config, path] if check == "--check-config" && config == "-c" => {
            Some((PathBuf::from(path), true))
        }
        [config, path, check] if config == "-c" && check == "--check-config" => {
            Some((PathBuf::from(path), true))
        }
        _ => None,
    }
}

fn check_node_config(config_path: PathBuf) -> ExitCode {
    let cfg = match NodeConfig::load(&config_path) {
        Ok(cfg) => cfg,
        Err(error) => {
            eprintln!("valsd node: {}: {error}", config_path.display());
            return ExitCode::from(2);
        }
    };
    let (settings, legacy) = match run_node::resolve_settings(&cfg) {
        Ok(resolved) => resolved,
        Err(error) => { eprintln!("valsd node: {error}"); return ExitCode::from(2); }
    };
    let (level, legacy_log) = match config::resolve_log_level(cfg.log_level) {
        Ok(resolved) => resolved,
        Err(error) => { eprintln!("valsd node: {error}"); return ExitCode::from(2); }
    };
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    match allocator::interval() {
        Ok(interval) => println!("effective allocator maintenance: malloc_trim_secs={}", interval.as_secs()),
        Err(error) => { eprintln!("valsd node: {error}"); return ExitCode::from(2); }
    }
    println!("effective node settings: {settings:?}");
    println!("effective transport limits: {:?}", settings.transport_limits());
    println!("effective node cadence: flush={:?} compact={:?}; log_level={level}",
        settings.flush_interval(cfg.sweep), settings.compact_interval(cfg.sweep));
    if !legacy.is_empty() { eprintln!("deprecated node environment settings: {}; migrate to config keys (docs/configuration.md)", legacy.join(", ")); }
    if legacy_log { eprintln!("deprecated VALS_LOG; migrate to log_level (docs/configuration.md)"); }
    println!("node config ok: {}", config_path.display());
    println!(
        "effective: node_id={} profile={} backend={:?} listeners={} boot_members={} rf={} \
         sweep={:?} gc_grace={:?}",
        cfg.node_id,
        cfg.profile,
        cfg.backend,
        cfg.listen.len(),
        cfg.peers.len() + 1,
        cfg.rf,
        cfg.sweep,
        cfg.gc_grace,
    );
    println!(
        "effective endpoints: listen={:?} peers={:?} metrics={:?} admin_socket={:?}",
        cfg.listen, cfg.peers, cfg.metrics, cfg.admin_socket
    );
    println!(
        "effective storage: data_dir={:?} pci={:?} spdk_shm_id={} spdk_mem_mb={} \
         spdk_core={}",
        cfg.data_dir, cfg.pci, cfg.spdk_shm_id, cfg.spdk_mem_mb, cfg.spdk_core
    );
    println!(
        "effective geometry: block_size={} seg_blocks={} region_mib={} zone_region_mib={} \
         zone_region_mib_overrides={:?}",
        cfg.block_size,
        cfg.seg_blocks,
        cfg.region_mib,
        cfg.zone_region_mib,
        cfg.zone_region_mib_overrides
    );
    println!(
        "effective controls: secret={} log_level={level}",
        if cfg.secret.is_some() { "configured" } else { "open" }
    );
    println!("reloadable: log_level, sweep; all other node settings require restart");
    if cfg.backend == config::Backend::Spdk {
        println!(
            "SPDK check is side-effect-free: devices were not attached or formatted; provision \
             offline with `valsd provision` before restart when a new layout is required"
        );
    }
    ExitCode::SUCCESS
}

fn check_pd_config(config_path: PathBuf) -> ExitCode {
    let cfg = match PdConfig::load(&config_path) {
        Ok(cfg) => cfg,
        Err(error) => {
            eprintln!("valsd pd: {}: {error}", config_path.display());
            return ExitCode::from(2);
        }
    };
    let (placement, legacy) = match run_pd::resolve_placement(&cfg) {
        Ok(resolved) => resolved,
        Err(error) => {
            eprintln!("valsd pd: {}: {error}", config_path.display());
            return ExitCode::from(2);
        }
    };
    let (level, legacy_log) = match config::resolve_log_level(cfg.log_level) {
        Ok(resolved) => resolved,
        Err(error) => { eprintln!("valsd pd: {error}"); return ExitCode::from(2); }
    };
    let transport = match vals_cluster::transport::TransportLimits::from_env() {
        Ok(limits) => limits,
        Err(error) => { eprintln!("valsd pd: {error}"); return ExitCode::from(2); }
    };
    println!("effective PD transport limits: {transport:?}; log_level={level}");
    if legacy_log { eprintln!("deprecated VALS_LOG; migrate to log_level (docs/configuration.md)"); }
    println!("effective PD placement settings: {placement:?}");
    if !legacy.is_empty() {
        eprintln!(
            "deprecated PD environment settings: {}; migrate to PD config keys (docs/configuration.md)",
            legacy.join(", "),
        );
    }
    println!("pd config ok: {}", config_path.display());
    println!(
        "effective: pd_id={} raft_mode={} data_nodes={} rf={} vnodes={} boot_zones={} \
         state_capacity_mib={}",
        cfg.pd_id,
        if cfg.pd_peers.is_empty() { "single" } else { "multi" },
        cfg.data_nodes.len(),
        cfg.rf,
        cfg.vnodes,
        cfg.zones.len(),
        cfg.state_capacity_mib,
    );
    println!(
        "effective raft: listen={:?} peers={:?} campaign={} data_dir={:?}",
        cfg.listen, cfg.pd_peers, cfg.campaign, cfg.data_dir
    );
    println!(
        "effective placement: data_nodes={:?} zones={:?}",
        cfg.data_nodes,
        cfg.zones
            .iter()
            .map(|zone| (&zone.name, zone.rf, zone.ordering))
            .collect::<Vec<_>>()
    );
    println!(
        "effective controls: admin_socket={:?} metrics={:?} secret={} log_level={level}",
        cfg.admin_socket,
        cfg.metrics,
        if cfg.secret.is_some() { "configured" } else { "open" }
    );
    println!("reloadable: log_level; all other PD settings require restart");
    println!("check mode did not bind sockets, create directories, campaign, or mutate PD state");
    ExitCode::SUCCESS
}

fn init_common(secret: &Option<Vec<u8>>, configured: Option<log::LevelFilter>) -> Result<(), config::ConfigError> {
    let (level, legacy) = config::resolve_log_level(configured)?;
    vals_cluster::observe::init_stderr_logger_at(level);
    log::info!("effective log_level={level}");
    if legacy { log::warn!("deprecated VALS_LOG; migrate to log_level (docs/configuration.md)"); }
    if let Some(secret) = secret {
        if !vals_cluster::init_cluster_secret(secret) {
            log::warn!("cluster secret was already set in this process; keeping the first");
        }
    } else {
        log::warn!(
            "no cluster secret configured: control ops are OPEN — set `secret_file` \
             on every node and PD in production"
        );
    }
    Ok(())
}

fn apply_log_level(level: Option<log::LevelFilter>) {
    if let Some(level) = level {
        log::set_max_level(level);
    }
}

fn node_main(config_path: PathBuf) -> ExitCode {
    let mut cfg = match NodeConfig::load(&config_path) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("valsd node: {}: {e}", config_path.display());
            return ExitCode::from(2);
        }
    };
    if let Err(error) = init_common(&cfg.secret, cfg.log_level) {
        eprintln!("valsd node: {error}"); return ExitCode::from(2);
    }
    let (settings, legacy) = match run_node::resolve_settings(&cfg) {
        Ok(resolved) => resolved,
        Err(error) => { eprintln!("valsd node: {error}"); return ExitCode::from(2); }
    };
    if !legacy.is_empty() { log::warn!("deprecated node environment settings: {}; migrate to config keys (docs/configuration.md)", legacy.join(", ")); }
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    let trim_interval = match allocator::interval() {
        Ok(interval) => interval,
        Err(error) => { eprintln!("valsd node: {error}"); return ExitCode::from(2); }
    };
    #[cfg(feature = "spdk")]
    let _device_locks = if cfg.backend == config::Backend::Spdk {
        match spdk_lock::DeviceLocks::acquire(&cfg.pci) {
            Ok(locks) => Some(locks),
            Err(e) => {
                eprintln!("valsd node: SPDK device ownership: {e}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        None
    };
    install_signal_handlers();
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    let _allocator_trimmer = {
        log::info!("allocator maintenance: malloc_trim_secs={}", trim_interval.as_secs());
        allocator::start(trim_interval)
    };
    let handle = match run_node::run_node(&cfg, settings) {
        Ok(handle) => handle,
        Err(e) => {
            log::error!("node {} failed to start: {e}", cfg.node_id);
            return ExitCode::FAILURE;
        }
    };
    let admin = match &cfg.admin_socket {
        Some(path) => match admin::spawn_listener(path) {
            Ok(rx) => {
                log::info!("node {}: admin socket at {}", cfg.node_id, path.display());
                Some((rx, path.clone()))
            }
            Err(e) => {
                log::error!(
                    "node {}: admin socket bind {} failed ({e}); admin channel disabled",
                    cfg.node_id,
                    path.display()
                );
                None
            }
        },
        None => None,
    };
    let running = || !SHUTDOWN.load(Ordering::SeqCst) && !integrity::fail_fast_requested();
    let sock_path = match admin {
        Some((admin_rx, sock_path)) => {
            let dumper = handle.dumper();
            while running() {
                if RELOAD.swap(false, Ordering::SeqCst) {
                    reload_node(&config_path, &mut cfg, &handle);
                }
                match admin_rx.recv_timeout(Duration::from_millis(50)) {
                    Ok((line, reply_tx)) => node_admin::dispatch(&dumper, &line, reply_tx),
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
            Some(sock_path)
        }
        None => {
            while running() {
                if RELOAD.swap(false, Ordering::SeqCst) {
                    reload_node(&config_path, &mut cfg, &handle);
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            None
        }
    };
    ExitCode::from(finish_shutdown(integrity::fail_fast_requested(), cfg.node_id,
        integrity::FAIL_FAST_SHUTDOWN_DEADLINE, || {
            handle.shutdown();
            if let Some(sock_path) = sock_path {
                let _ = std::fs::remove_file(&sock_path);
            }
        }))
}

fn finish_shutdown(fail_fast: bool, node_id: u32, deadline: Duration, shutdown: impl FnOnce()) -> u8 {
    if fail_fast {
        arm_fail_fast_watchdog(deadline);
        log::error!(
            "node {}: fail-fast: graceful shutdown without a final memtable flush, then exit {} \
             (hard exit after {:?})",
            node_id, integrity::INTEGRITY_FAIL_FAST_EXIT_CODE, deadline
        );
    } else {
        log::info!("node {}: signal received, shutting down", node_id);
    }
    shutdown();
    if fail_fast {
        log::error!(
            "node {}: fail-fast: shutdown complete; exiting with code {}",
            node_id, integrity::INTEGRITY_FAIL_FAST_EXIT_CODE
        );
        log::logger().flush();
        integrity::INTEGRITY_FAIL_FAST_EXIT_CODE
    } else { 0 }
}

fn arm_fail_fast_watchdog(deadline: Duration) {
    let spawned = std::thread::Builder::new().name("vals-fail-fast".into()).spawn(move || {
        std::thread::sleep(deadline);
        hard_fail_fast_exit(b"valsd: fail-fast watchdog deadline exceeded; exiting 75\n");
    });
    if spawned.is_err() {
        hard_fail_fast_exit(b"valsd: fail-fast watchdog spawn failed; exiting 75\n");
    }
}

fn hard_fail_fast_exit(message: &[u8]) -> ! {
    let message = &message[..message.len().min(libc::PIPE_BUF)];
    let mut pfd = libc::pollfd { fd: libc::STDERR_FILENO, events: libc::POLLOUT, revents: 0 };
    // SAFETY: poll and write receive a valid pollfd and a live buffer of the given length; _exit never returns
    // and runs no destructors or atexit handlers, so it cannot block on logger or stdio locks.
    unsafe {
        if libc::poll(&mut pfd, 1, 0) == 1 && pfd.revents & libc::POLLOUT != 0 {
            libc::write(libc::STDERR_FILENO, message.as_ptr().cast(), message.len());
        }
        libc::_exit(integrity::INTEGRITY_FAIL_FAST_EXIT_CODE as libc::c_int)
    }
}

fn reload_node(
    config_path: &std::path::Path,
    cfg: &mut NodeConfig,
    handle: &vals_cluster::node::NodeHandle,
) {
    let new = match NodeConfig::load(config_path) {
        Ok(new) => new,
        Err(e) => {
            log::error!(
                "node {}: config reload failed ({}: {e}); keeping the running config",
                cfg.node_id,
                config_path.display()
            );
            return;
        }
    };

    let mut applied: Vec<&str> = Vec::new();

    if new.log_level != cfg.log_level {
        apply_log_level(new.log_level);
        if let Some(level) = new.log_level {
            log::info!("node {}: log level now {level}", cfg.node_id);
        }
        cfg.log_level = new.log_level;
        applied.push("log_level");
    }

    if new.sweep != cfg.sweep {
        log::info!(
            "node {}: sweep cadence {:?} -> {:?} (also retunes compaction/flush)",
            cfg.node_id,
            cfg.sweep,
            new.sweep
        );
        handle.set_sweep_every(new.sweep);
        cfg.sweep = new.sweep;
        applied.push("sweep");
    }

    let ignored = node_nonreloadable_changes(cfg, &new);
    for key in &ignored {
        log::warn!(
            "node {}: `{key}` changed but is not reloadable; keeping the running value \
             (restart to apply)",
            cfg.node_id
        );
    }

    log::info!(
        "node {}: config reloaded (applied: {applied:?}; ignored non-reloadable: {ignored:?})",
        cfg.node_id
    );
}

fn node_nonreloadable_changes(cur: &NodeConfig, new: &NodeConfig) -> Vec<&'static str> {
    let mut changed = Vec::new();
    macro_rules! cmp {
        ($field:ident, $name:literal) => {
            if cur.$field != new.$field {
                changed.push($name);
            }
        };
    }
    cmp!(settings, "node settings");
    cmp!(node_id, "node_id");
    cmp!(profile, "profile");
    cmp!(listen, "listen");
    cmp!(backend, "backend");
    cmp!(data_dir, "data_dir");
    cmp!(peers, "peer");
    cmp!(rf, "rf");
    cmp!(gc_grace, "gc_grace");
    cmp!(secret, "secret");
    cmp!(block_size, "block_size");
    cmp!(seg_blocks, "seg_blocks");
    cmp!(region_mib, "region_mib");
    cmp!(zone_region_mib, "zone_region_mib");
    cmp!(
        zone_region_mib_overrides,
        "zone_region_mib_override"
    );
    cmp!(pci, "pci");
    cmp!(spdk_shm_id, "spdk_shm_id");
    cmp!(spdk_mem_mb, "spdk_mem_mb");
    cmp!(spdk_core, "spdk_core");
    cmp!(metrics, "metrics");
    cmp!(admin_socket, "admin_socket");
    changed
}

fn pd_main(config_path: PathBuf) -> ExitCode {
    let mut cfg = match PdConfig::load(&config_path) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("valsd pd: {}: {e}", config_path.display());
            return ExitCode::from(2);
        }
    };
    if let Err(error) = init_common(&cfg.secret, cfg.log_level) {
        eprintln!("valsd pd: {error}"); return ExitCode::from(2);
    }
    install_signal_handlers();
    let (pd, admin_rx) = match run_pd::run_pd(&cfg) {
        Ok(v) => v,
        Err(e) => {
            log::error!("pd {} failed to start: {e}", cfg.pd_id);
            return ExitCode::FAILURE;
        }
    };
    while !SHUTDOWN.load(Ordering::SeqCst) {
        if RELOAD.swap(false, Ordering::SeqCst) {
            reload_pd(&config_path, &mut cfg);
        }
        match admin_rx.recv_timeout(Duration::from_millis(50)) {
            Ok((line, reply_tx)) => {
                let _ = reply_tx.send(admin::handle_command(&pd, &line));
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    log::info!("pd {}: signal received, shutting down", cfg.pd_id);
    pd.shutdown();
    let _ = std::fs::remove_file(&cfg.admin_socket);
    ExitCode::SUCCESS
}

fn reload_pd(config_path: &std::path::Path, cfg: &mut PdConfig) {
    let new = match PdConfig::load(config_path) {
        Ok(new) => new,
        Err(e) => {
            log::error!(
                "pd {}: config reload failed ({}: {e}); keeping the running config",
                cfg.pd_id,
                config_path.display()
            );
            return;
        }
    };

    let mut applied: Vec<&str> = Vec::new();
    if new.log_level != cfg.log_level {
        apply_log_level(new.log_level);
        if let Some(level) = new.log_level {
            log::info!("pd {}: log level now {level}", cfg.pd_id);
        }
        cfg.log_level = new.log_level;
        applied.push("log_level");
    }

    let ignored = pd_nonreloadable_changes(cfg, &new);
    for key in &ignored {
        log::warn!(
            "pd {}: `{key}` changed but is not reloadable; keeping the running value \
             (restart to apply)",
            cfg.pd_id
        );
    }

    log::info!(
        "pd {}: config reloaded (applied: {applied:?}; ignored non-reloadable: {ignored:?})",
        cfg.pd_id
    );
}

fn pd_nonreloadable_changes(cur: &PdConfig, new: &PdConfig) -> Vec<&'static str> {
    let mut changed = Vec::new();
    macro_rules! cmp {
        ($field:ident, $name:literal) => {
            if cur.$field != new.$field {
                changed.push($name);
            }
        };
    }
    cmp!(pd_id, "pd_id");
    cmp!(data_dir, "data_dir");
    cmp!(listen, "listen");
    cmp!(pd_peers, "pd_peer");
    cmp!(campaign, "campaign");
    cmp!(data_nodes, "data_node");
    cmp!(rf, "rf");
    cmp!(vnodes, "vnodes");
    cmp!(state_capacity_mib, "state_capacity_mib");
    cmp!(placement, "placement settings");
    cmp!(admin_socket, "admin_socket");
    cmp!(secret, "secret");
    cmp!(metrics, "metrics");
    if cur.zones.len() != new.zones.len()
        || cur
            .zones
            .iter()
            .zip(&new.zones)
            .any(|(a, b)| a.name != b.name || a.rf != b.rf || a.ordering != b.ordering)
    {
        changed.push("zone");
    }
    changed
}

fn admin_main(socket: PathBuf, cmd: &[String]) -> ExitCode {
    match admin::client(&socket, cmd) {
        Ok(code) => ExitCode::from(code),
        Err(e) => {
            eprintln!("valsd admin: {}: {e}", socket.display());
            ExitCode::FAILURE
        }
    }
}
