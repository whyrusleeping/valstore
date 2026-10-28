use std::time::{Duration, Instant};

use vals_spdk::{AsyncRead, BlockDevice, FileBlockDevice, MemBlockDevice};
use vals_store::{Config, Engine, Ordering, RunBuildOpts};


mod common;
use common::*;
mod engine;
use engine::*;
mod spdk_device;
use spdk_device::*;
mod cluster_live;
use cluster_live::*;
mod cluster_passive;
use cluster_passive::*;
mod models;
use models::*;
mod profilecheck;
use profilecheck::*;
mod capacity;
use capacity::*;
mod seed_mass;
use seed_mass::*;
mod netload;
use netload::*;
mod seed_verify;
use seed_verify::*;
mod rtqueue;
use rtqueue::*;
mod netqueue;
use netqueue::*;
mod rtdiag;
mod graph;
mod backupdrill;

fn node_settings() -> &'static vals_cluster::node_settings::NodeSettings {
    static SETTINGS: std::sync::OnceLock<vals_cluster::node_settings::NodeSettings> = std::sync::OnceLock::new();
    SETTINGS.get_or_init(|| {
        let (settings, legacy) = vals_cluster::node_settings::NodeSettingsOverrides::default()
            .resolve(|name| std::env::var_os(name).map(|value| value.to_string_lossy().into_owned()))
            .expect("invalid node environment settings");
        let settings = settings.resolve_ram_defaults();
        settings.configure_process().expect("conflicting node process settings");
        println!("effective node settings: {settings:?}");
        if !legacy.is_empty() { eprintln!("deprecated node environment settings: {}", legacy.join(", ")); }
        settings
    })
}

fn net_mode_args(args: &[String]) -> (usize, usize) {
    let rf = args.get(7).and_then(|s| s.parse().ok()).unwrap_or(2);
    let ncoord = args.get(8).and_then(|s| s.parse().ok()).unwrap_or(8);
    (rf, ncoord)
}

#[cfg(feature = "spdk")]
fn require_spdk(backend: &str, message: &str) {
    if backend != "spdk" {
        eprintln!("{message}");
        std::process::exit(2);
    }
}

macro_rules! workloads {
    (
        match ($workload:expr) usage($usage:ident) {
            $(
                $(#[$meta:meta])*
                $name:literal => $body:block
            )*
            ; $other:ident => $fallback:block
        }
    ) => {{
        const $usage: &[&str] = &[
            $($(#[$meta])* $name,)*
        ];
        match $workload {
            $($(#[$meta])* $name => $body,)*
            $other => $fallback,
        }
    }};
}

fn main() {
    vals_cluster::observe::init_stderr_logger();
    let args: Vec<String> = std::env::args().collect();
    let backend = args.get(1).map(String::as_str).unwrap_or("mem");
    let workload = args.get(2).map(String::as_str).unwrap_or("write");
    let n: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(200_000);
    let vsize: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(100);
    let default_path = std::env::temp_dir()
        .join("vals-bench.img")
        .to_string_lossy()
        .into_owned();
    let path = args.get(5).map(String::as_str).unwrap_or(&default_path);
    let batch: usize = args.get(6).and_then(|s| s.parse().ok()).unwrap_or(64);
    #[cfg(feature = "spdk")]
    let arg7: usize = args.get(7).and_then(|s| s.parse().ok()).unwrap_or(4);

    println!("vals-bench: backend={backend} workload={workload} n={n} vsize={vsize}");
    if backend == "spdk" {
        println!("(real NVMe via SPDK)\n");
    } else {
        println!("(software-path numbers over the {backend} backend)\n");
    }

    let (net_rf, net_ncoord) = net_mode_args(&args);

    workloads! {
        match (workload) usage(WORKLOADS) {
        "graph" => {
            graph::local(backend, path, n, vsize, batch)
        }
        "netgraph" => {
            graph::network(path, n, vsize, batch)
        }
        "write" => {
            workload_write(&mut open_engine(backend, path, n, vsize), n, vsize)
        }
        "read" => {
            workload_read(&mut open_engine(backend, path, n, vsize), n, vsize)
        }
        "mixed" => {
            workload_mixed(&mut open_engine(backend, path, n, vsize), n, vsize)
        }
        "memprof" => {
            workload_memprof(backend, path, n, vsize)
        }
        "capacity" => {
            workload_capacity(backend, path, n, vsize, batch)
        }
        "seedmass" => {
            assert_eq!(backend, "file");
            workload_seed_mass(path, n, vsize, batch)
        }
        "scanlsm" => {
            let nruns: usize = args.get(7).and_then(|s| s.parse().ok()).unwrap_or(4);
            let order = args.get(8).map(String::as_str).unwrap_or("shuffled");
            workload_scanlsm(backend, path, n, vsize, batch, nruns, order)
        }
        "cocompact" => {
            let nruns: usize = args.get(7).and_then(|s| s.parse().ok()).unwrap_or(4);
            let order = args.get(8).map(String::as_str).unwrap_or("shuffled");
            workload_cocompact(backend, path, n, vsize, batch, nruns, order)
        }
        "batchread" => {
            workload_batchread(&mut open_engine(backend, path, n, vsize), n, vsize, batch)
        }
        "profilecheck" => {
            let seconds = args.get(6).and_then(|s| s.parse::<u64>().ok()).filter(|s| *s > 0);
            workload_profilecheck(path, n, seconds)
        }
        "netseedcheck" => {
            if let Err(error) = workload_seed_verify(path, n as u32) {
                eprintln!("SEED_DIGEST_CHECK status=fail reason={error}");
                std::process::exit(1);
            }
        }
        "netload" => {
            workload_netload(
                path,
                n,
                vsize,
                batch,
                net_rf,
                net_ncoord,
                true,
                1,
                1,
                vals_cluster::Persistence::Buffered,
            )
        }
        "netread" => {
            workload_netload(
                path,
                n,
                vsize,
                batch,
                net_rf,
                net_ncoord,
                false,
                1,
                1,
                vals_cluster::Persistence::Buffered,
            )
        }
        "netmultiread" => {
            let read_batch_width = netmultiread_batch_width(
                std::env::var("VALS_NETMULTIREAD_BATCH").ok().as_deref(),
            );
            workload_netload(
                path,
                n,
                vsize,
                batch,
                net_rf,
                net_ncoord,
                false,
                read_batch_width,
                1,
                vals_cluster::Persistence::Buffered,
            )
        }
        "netbatchwrite" => {
            let width = netbatchwrite_width(
                std::env::var("VALS_NETBATCHWRITE_WIDTH").ok().as_deref(),
            );
            let persistence = netbatchwrite_persistence(
                std::env::var("VALS_NETBATCHWRITE_PERSISTENCE")
                    .ok()
                    .as_deref(),
            );
            workload_netload(
                path,
                n,
                vsize,
                batch,
                net_rf,
                net_ncoord,
                true,
                1,
                width,
                persistence,
            )
        }
        #[cfg(feature = "spdk")]
        "asyncread" => {
            require_spdk(
                backend,
                "asyncread requires the spdk backend + a PCIe addr as the path arg",
            );
            workload_asyncread(path, n, batch)
        }
        #[cfg(feature = "spdk")]
        "asyncwrite" => {
            require_spdk(
                backend,
                "asyncwrite requires the spdk backend + a PCIe addr as the path arg",
            );
            workload_asyncwrite(path, n, batch)
        }
        #[cfg(feature = "spdk")]
        "syncwrite" => {
            require_spdk(
                backend,
                "syncwrite requires the spdk backend + a PCIe addr as the path arg",
            );
            workload_syncwrite(path, n)
        }
        #[cfg(feature = "spdk")]
        "shardwrite" => {
            require_spdk(
                backend,
                "shardwrite requires the spdk backend + a PCIe addr as the path arg",
            );
            workload_shardwrite(path, n, vsize, batch)
        }
        #[cfg(feature = "spdk")]
        "shardread" => {
            require_spdk(
                backend,
                "shardread requires the spdk backend + a PCIe addr as the path arg",
            );
            workload_shardread(path, n, vsize, batch, 128)
        }
        #[cfg(feature = "spdk")]
        "mdwrite" => {
            require_spdk(
                backend,
                "mdwrite requires spdk + a comma-separated PCIe list as the path arg",
            );
            workload_mdwrite(path, n, vsize, batch)
        }
        #[cfg(feature = "spdk")]
        "mdread" => {
            require_spdk(
                backend,
                "mdread requires spdk + a comma-separated PCIe list as the path arg",
            );
            workload_mdread(path, n, vsize, batch)
        }
        #[cfg(feature = "spdk")]
        "distspdk" => {
            require_spdk(
                backend,
                "distspdk requires spdk + a comma-separated PCIe list (>=6) as the path arg",
            );
            workload_distspdk(path, n, vsize)
        }
        #[cfg(feature = "spdk")]
        "distspdkmc" => {
            require_spdk(
                backend,
                "distspdkmc requires spdk + a comma-separated PCIe list (>=6)",
            );
            workload_distspdkmc(path, n, vsize, batch)
        }
        #[cfg(feature = "spdk")]
        "distmdmc" => {
            require_spdk(
                backend,
                "distmdmc requires spdk + a comma-separated PCIe list (multiple of 2)",
            );
            workload_distmdmc(path, n, vsize, batch, arg7)
        }
        #[cfg(feature = "spdk")]
        "distbatch" => {
            require_spdk(
                backend,
                "distbatch requires spdk + a comma-separated PCIe list (>=6)",
            );
            workload_distbatch(path, n, vsize, batch)
        }
        #[cfg(feature = "spdk")]
        "distzone" => {
            require_spdk(
                backend,
                "distzone requires spdk + a comma-separated PCIe list (>=6)",
            );
            workload_distzone(path, n, vsize)
        }
        #[cfg(feature = "spdk")]
        "distsorted" => {
            require_spdk(
                backend,
                "distsorted requires spdk + a comma-separated PCIe list (>= RF*dpn drives; RF via VALS_DS_RF, default 3)",
            );
            workload_distsorted(path, n, vsize, batch, arg7)
        }
        #[cfg(feature = "spdk")]
        "distmd" => {
            require_spdk(
                backend,
                "distmd requires spdk + a comma-separated PCIe list (the node's drives)",
            );
            workload_distmd(path, n, vsize, batch)
        }
        #[cfg(feature = "spdk")]
        "distmdmove" => {
            require_spdk(
                backend,
                "distmdmove requires spdk + a comma-separated PCIe list (>= 2 drives)",
            );
            workload_distmdmove(path, n, vsize, batch)
        }
        #[cfg(feature = "spdk")]
        "distmdfail" => {
            require_spdk(
                backend,
                "distmdfail requires spdk + a comma-separated PCIe list (>= 7 drives)",
            );
            workload_distmdfail(path, n, vsize, batch)
        }
        #[cfg(feature = "spdk")]
        "distresil" => {
            require_spdk(
                backend,
                "distresil requires spdk + a comma-separated PCIe list (>=5, one drive/node)",
            );
            workload_distresil(path, vsize)
        }
        #[cfg(feature = "spdk")]
        "engineasync" => {
            require_spdk(
                backend,
                "engineasync requires the spdk backend + a PCIe addr as the path arg",
            );
            workload_engineasync(path, n, vsize, batch)
        }
        "recover" => {
            workload_recover(backend, path, n, vsize)
        }
        "distnode" => {
            #[cfg(feature = "spdk")]
            if backend == "spdk" {
                workload_distnode_spdk(path, n, vsize);
                return;
            }
            if backend == "spdk" {
                eprintln!("distnode on the spdk backend needs a `--features spdk` build");
                std::process::exit(2);
            }
            workload_distnode_mem(n, vsize)
        }
        "rtqueue" => {
            workload_rtqueue(backend, path, vsize)
        }
        "netqueue" => {
            workload_netqueue(
                backend,
                args.get(3).map(String::as_str),
                args.get(4).map(String::as_str),
            )
        }
        "rtdiag" => {
            rtdiag::workload_rtdiag(&args)
        }
        "backupdrill" => {
            backupdrill::workload_backupdrill(&args)
        }
        "pipewrite" => {
            workload_pipewrite(n, vsize, batch)
        }
        "mcread" => {
            workload_mcread(n, vsize, batch)
        }
        "dist" => {
            workload_dist(n, vsize, batch)
        }
        "distmc" => {
            workload_distmc(n, vsize, batch)
        }
        "distmcp" => {
            workload_distmcp(n, vsize, batch)
        }
        "disthedge" => {
            workload_disthedge(n, vsize)
        }
        "distshard" => {
            workload_distshard(n, vsize, batch)
        }
        ; other => {
            eprintln!(
                "unknown workload '{other}' (expected {})",
                WORKLOADS.join(" | ")
            );
            std::process::exit(2);
        }
        }
    }
}
