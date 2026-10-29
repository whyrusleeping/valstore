use super::*;

#[cfg(feature = "spdk")]
pub(super) fn workload_asyncread(pci: &str, n: usize, concurrency: usize) {
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use vals_rt::LocalExecutor;
    use vals_spdk::spdk::{SpdkBlockDevice, SpdkConfig};

    let dev = Rc::new(
        SpdkBlockDevice::open(&SpdkConfig {
            pci_addr: pci.to_string(),
            nsid: 1,
        })
        .expect("open spdk nvme"),
    );
    let bs = dev.block_size();
    let region = dev.num_blocks().min(1 << 20);

    {
        let pattern = make_value(bs, 0xABCD);
        dev.write_at(0, &pattern).unwrap();
        dev.flush().unwrap();
        let ok = Rc::new(Cell::new(false));
        let ex = LocalExecutor::new();
        let d = dev.clone();
        let okc = ok.clone();
        ex.spawn(async move {
            let mut b = vec![0u8; bs];
            // SAFETY: `b` is owned by this task, so leaking the read leaks it too.
            unsafe { d.read_at_async(0, &mut b) }.await.unwrap();
            okc.set(b == pattern);
        });
        let dp = dev.clone();
        ex.run(move || {
            dp.poll();
        });
        assert!(ok.get(), "async read-back mismatch");
        println!("  correctness  : ok");
    }

    {
        let src = make_value(bs, 0x1234);
        let counter = Rc::new(Cell::new(0u64));
        let ex = LocalExecutor::new();
        for _ in 0..concurrency.max(1) {
            let dev = dev.clone();
            let counter = counter.clone();
            let src = src.clone();
            ex.spawn(async move {
                loop {
                    let i = counter.get();
                    if i >= region {
                        break;
                    }
                    counter.set(i + 1);
                    dev.write_at_async(i, &src).await.unwrap();
                }
            });
        }
        let dp = dev.clone();
        ex.run(move || {
            dp.poll();
        });
        dev.flush().unwrap();
    }

    let counter = Rc::new(Cell::new(0usize));
    let lat = Rc::new(RefCell::new(Vec::with_capacity(n)));
    let ex = LocalExecutor::new();
    let start = Instant::now();
    for w in 0..concurrency {
        let dev = dev.clone();
        let counter = counter.clone();
        let lat = lat.clone();
        ex.spawn(async move {
            let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (w as u64).wrapping_mul(0x1000_0001B));
            let mut buf = vec![0u8; bs];
            loop {
                let i = counter.get();
                if i >= n {
                    break;
                }
                counter.set(i + 1);
                let lba = rng.below(region);
                let t = Instant::now();
                // SAFETY: `buf` is owned by this task, so leaking the read leaks it too.
                unsafe { dev.read_at_async(lba, &mut buf) }.await.unwrap();
                lat.borrow_mut().push(t.elapsed().as_nanos() as u64);
            }
        });
    }
    let dp = dev.clone();
    ex.run(move || {
        dp.poll();
    });
    let secs = start.elapsed().as_secs_f64();
    let lat = Rc::try_unwrap(lat).unwrap().into_inner();
    report(
        &format!("asyncread (concurrency={concurrency})"),
        lat,
        secs,
        (n * bs) as u64,
    );
}

#[cfg(feature = "spdk")]
pub(super) fn workload_syncwrite(pci: &str, n: usize) {
    use vals_spdk::spdk::{SpdkBlockDevice, SpdkConfig};

    let dev = SpdkBlockDevice::open(&SpdkConfig {
        pci_addr: pci.to_string(),
        nsid: 1,
    })
    .expect("open spdk nvme");
    let bs = dev.block_size();
    let unit = 4096.max(bs);
    let spp = (unit / bs) as u64;
    let num_units = dev.num_blocks() / spp;

    let buf = make_value(unit, 0x77);
    let mut rng = Rng(0xA5A5_5A5A_C3C3_3C3C);
    let mut lat = Vec::with_capacity(n);
    let start = Instant::now();
    for _ in 0..n {
        let lba = rng.below(num_units) * spp;
        let t = Instant::now();
        dev.write_at(lba, &buf).unwrap();
        lat.push(t.elapsed().as_nanos() as u64);
    }
    let secs = start.elapsed().as_secs_f64();
    dev.flush().unwrap();
    report(
        &format!("syncwrite QD1 (unit={unit}B)"),
        lat,
        secs,
        (n * unit) as u64,
    );
}

#[cfg(feature = "spdk")]
pub(super) fn workload_asyncwrite(pci: &str, n: usize, concurrency: usize) {
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use vals_rt::LocalExecutor;
    use vals_spdk::spdk::{SpdkBlockDevice, SpdkConfig};

    let dev = Rc::new(
        SpdkBlockDevice::open(&SpdkConfig {
            pci_addr: pci.to_string(),
            nsid: 1,
        })
        .expect("open spdk nvme"),
    );
    let bs = dev.block_size();
    let unit = 4096.max(bs);
    let spp = (unit / bs) as u64;
    let num_units = dev.num_blocks() / spp;

    {
        let pattern = make_value(unit, 0x5151);
        let ex = LocalExecutor::new();
        let d = dev.clone();
        let pat = pattern.clone();
        ex.spawn(async move {
            d.write_at_async(0, &pat).await.unwrap();
        });
        let dp = dev.clone();
        ex.run(move || {
            dp.poll();
        });
        dev.flush().unwrap();
        let mut b = vec![0u8; unit];
        dev.read_at(0, &mut b).unwrap();
        assert!(b == pattern, "async write read-back mismatch");
        println!("  correctness  : ok");
    }

    let counter = Rc::new(Cell::new(0usize));
    let lat = Rc::new(RefCell::new(Vec::with_capacity(n)));
    let ex = LocalExecutor::new();
    let start = Instant::now();
    for w in 0..concurrency {
        let dev = dev.clone();
        let counter = counter.clone();
        let lat = lat.clone();
        ex.spawn(async move {
            let mut rng = Rng(0xD1B5_4A32_D192_ED03 ^ (w as u64).wrapping_mul(0x1_0000_001B));
            let buf = make_value(unit, w as u64);
            loop {
                let i = counter.get();
                if i >= n {
                    break;
                }
                counter.set(i + 1);
                let lba = rng.below(num_units) * spp;
                let t = Instant::now();
                dev.write_at_async(lba, &buf).await.unwrap();
                lat.borrow_mut().push(t.elapsed().as_nanos() as u64);
            }
        });
    }
    let dp = dev.clone();
    ex.run(move || {
        dp.poll();
    });
    let secs = start.elapsed().as_secs_f64();
    dev.flush().unwrap();
    let lat = Rc::try_unwrap(lat).unwrap().into_inner();
    report(
        &format!("asyncwrite (concurrency={concurrency}, unit={unit}B)"),
        lat,
        secs,
        (n * unit) as u64,
    );
}

#[cfg(feature = "spdk")]
pub(super) fn workload_shardwrite(pci: &str, n: usize, vsize: usize, shards: usize) {
    use std::sync::{Arc, Barrier};
    use vals_spdk::spdk::{SpdkConfig, SpdkController};

    let shards = shards.max(1);
    let ctrlr = SpdkController::attach(&SpdkConfig {
        pci_addr: pci.to_string(),
        nsid: 1,
    })
    .expect("attach controller");
    let per_shard_blocks = ctrlr.num_blocks() / shards as u64;
    let per_n = n / shards;
    let barrier = Arc::new(Barrier::new(shards));

    let handles: Vec<_> = (0..shards)
        .map(|s| {
            let ctrlr = ctrlr.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let dev = ctrlr
                    .open_shard(s as u64 * per_shard_blocks, per_shard_blocks)
                    .expect("open shard");
                let cfg = bench_config(Ordering::Unsorted);
                let mut e = Engine::create(dev, cfg).unwrap();
                let keys: Vec<String> = (0..per_n).map(|i| format!("s{s}-{i:012}")).collect();
                let val = make_value(vsize, s as u64);
                barrier.wait();
                let start = Instant::now();
                for k in &keys {
                    e.put(k.as_bytes(), &val, 0).unwrap();
                }
                e.sync().unwrap();
                let secs = start.elapsed().as_secs_f64();
                assert_eq!(
                    e.get(keys[per_n / 2].as_bytes()).unwrap().as_deref(),
                    Some(val.as_slice())
                );
                secs
            })
        })
        .collect();

    let max_secs = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .fold(0.0f64, f64::max);
    let total = per_n * shards;
    println!("--- shardwrite (shards={shards}, vsize={vsize}) ---");
    println!("  ops          : {total}");
    println!("  wall         : {max_secs:.3} s");
    println!("  throughput   : {:.0} ops/s", total as f64 / max_secs);
    println!(
        "  bandwidth    : {:.1} MiB/s",
        (total * vsize) as f64 / max_secs / (1 << 20) as f64
    );
}

#[cfg(feature = "spdk")]
pub(super) fn workload_shardread(pci: &str, n: usize, vsize: usize, shards: usize, qd: usize) {
    use std::sync::{Arc, Barrier};
    use vals_spdk::spdk::{SpdkConfig, SpdkController};

    let shards = shards.max(1);
    let qd = qd.max(1);
    let ctrlr = SpdkController::attach(&SpdkConfig {
        pci_addr: pci.to_string(),
        nsid: 1,
    })
    .expect("attach controller");
    let per_shard_blocks = ctrlr.num_blocks() / shards as u64;
    let per_n = n / shards;
    let barrier = Arc::new(Barrier::new(shards));

    let handles: Vec<_> = (0..shards)
        .map(|s| {
            let ctrlr = ctrlr.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let dev = ctrlr
                    .open_shard(s as u64 * per_shard_blocks, per_shard_blocks)
                    .expect("open shard");
                let cfg = bench_config(Ordering::Unsorted);
                let mut e = Engine::create(dev, cfg).unwrap();
                let keys: Vec<String> = (0..per_n).map(|i| format!("s{s}-{i:012}")).collect();
                let val = make_value(vsize, s as u64);
                for k in &keys {
                    e.put(k.as_bytes(), &val, 0).unwrap();
                }
                e.sync().unwrap();
                let mut rng = Rng(0x2545_F491_4F6C_DD1D ^ (s as u64).wrapping_mul(0x9E37_79B9));
                let idxs: Vec<usize> = (0..per_n)
                    .map(|_| rng.below(per_n as u64) as usize)
                    .collect();
                barrier.wait();
                let start = Instant::now();
                let mut done = 0;
                while done < per_n {
                    let this = qd.min(per_n - done);
                    let refs: Vec<&[u8]> = idxs[done..done + this]
                        .iter()
                        .map(|&i| keys[i].as_bytes())
                        .collect();
                    let got = vals_rt::block_on(e.multi_get_async(&refs), || {
                        e.device().poll();
                    })
                    .unwrap();
                    debug_assert!(got.iter().all(|g| g.is_some()));
                    done += this;
                }
                start.elapsed().as_secs_f64()
            })
        })
        .collect();

    let max_secs = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .fold(0.0f64, f64::max);
    let total = per_n * shards;
    println!("--- shardread (shards={shards}, qd={qd}, vsize={vsize}) ---");
    println!("  ops          : {total}");
    println!("  wall         : {max_secs:.3} s");
    println!("  throughput   : {:.0} ops/s", total as f64 / max_secs);
}

#[cfg(feature = "spdk")]
fn pin_to_cpu(cpu: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

#[cfg(feature = "spdk")]
fn pin_cpu_for(strategy: &str, drive: usize, r: usize, ppd: usize, part: usize) -> Option<usize> {
    const CCDS: usize = 8;
    const CORES_PER_CCD: usize = 16;
    match strategy {
        "spread" => {
            let ccd = part % CCDS;
            let within = (part / CCDS) % CORES_PER_CCD;
            Some(ccd * CORES_PER_CCD + within)
        }
        "drive" => {
            let ccd = drive % CCDS;
            let within = r % CORES_PER_CCD;
            if ppd <= CORES_PER_CCD {
                Some(ccd * CORES_PER_CCD + within)
            } else {
                Some((part % CCDS) * CORES_PER_CCD + (part / CCDS) % CORES_PER_CCD)
            }
        }
        _ => None,
    }
}

#[cfg(feature = "spdk")]
fn run_multidrive<F>(pci_list: &str, ppd: usize, label: &str, vsize: usize, total_ops: usize, op: F)
where
    F: Fn(usize, &mut Engine<vals_spdk::spdk::SpdkBlockDevice>, usize) -> f64
        + Send
        + Sync
        + 'static,
{
    use std::sync::{Arc, Barrier};
    use vals_spdk::spdk::{SpdkConfig, SpdkController};

    let pcis: Vec<String> = pci_list
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let drives = pcis.len().max(1);
    let ppd = ppd.max(1);
    let nparts = drives * ppd;
    let per_part = total_ops / nparts;

    let ctrlrs: Vec<Arc<SpdkController>> = pcis
        .iter()
        .map(|p| {
            SpdkController::attach(&SpdkConfig {
                pci_addr: p.clone(),
                nsid: 1,
            })
            .expect("attach controller")
        })
        .collect();

    let pin = std::env::var("VALS_PIN").unwrap_or_default();
    let barrier = Arc::new(Barrier::new(nparts));
    let op = Arc::new(op);
    let mut handles = Vec::new();
    for (d, ctrlr) in ctrlrs.iter().enumerate() {
        let region = ctrlr.num_blocks() / ppd as u64;
        for r in 0..ppd {
            let ctrlr = ctrlr.clone();
            let barrier = barrier.clone();
            let op = op.clone();
            let part = d * ppd + r;
            let pin = pin.clone();
            handles.push(std::thread::spawn(move || {
                if let Some(cpu) = pin_cpu_for(&pin, d, r, ppd, part) {
                    pin_to_cpu(cpu);
                }
                let dev = ctrlr
                    .open_shard(r as u64 * region, region)
                    .expect("open shard");
                let cfg = bench_config(Ordering::Unsorted);
                let mut e = Engine::create(dev, cfg).unwrap();
                barrier.wait();
                op(part, &mut e, per_part)
            }));
        }
    }
    let max_secs = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .fold(0.0f64, f64::max);
    let total = per_part * nparts;
    let pin_label = if pin.is_empty() { "off" } else { pin.as_str() };
    println!(
        "--- {label} (drives={drives}, ppd={ppd}, parts={nparts}, vsize={vsize}, pin={pin_label}) ---"
    );
    println!("  ops          : {total}");
    println!("  wall         : {max_secs:.3} s");
    println!("  throughput   : {:.0} ops/s", total as f64 / max_secs);
    println!(
        "  bandwidth    : {:.1} MiB/s",
        (total * vsize) as f64 / max_secs / (1 << 20) as f64
    );
}

#[cfg(feature = "spdk")]
pub(super) fn workload_mdwrite(pci_list: &str, n: usize, vsize: usize, ppd: usize) {
    run_multidrive(
        pci_list,
        ppd,
        "mdwrite",
        vsize,
        n,
        move |part, e, per_part| {
            let keys: Vec<String> = (0..per_part).map(|i| format!("p{part}-{i:012}")).collect();
            let val = make_value(vsize, part as u64);
            let start = Instant::now();
            for k in &keys {
                e.put(k.as_bytes(), &val, 0).unwrap();
            }
            e.sync().unwrap();
            let secs = start.elapsed().as_secs_f64();
            assert_eq!(
                e.get(keys[per_part / 2].as_bytes()).unwrap().as_deref(),
                Some(val.as_slice())
            );
            secs
        },
    );
}

#[cfg(feature = "spdk")]
pub(super) fn workload_mdread(pci_list: &str, n: usize, vsize: usize, ppd: usize) {
    run_multidrive(
        pci_list,
        ppd,
        "mdread",
        vsize,
        n,
        move |part, e, per_part| {
            let keys: Vec<String> = (0..per_part).map(|i| format!("p{part}-{i:012}")).collect();
            let val = make_value(vsize, part as u64);
            for k in &keys {
                e.put(k.as_bytes(), &val, 0).unwrap();
            }
            e.sync().unwrap();
            let mut rng = Rng(0x2545_F491_4F6C_DD1D ^ (part as u64).wrapping_mul(0x9E37_79B9));
            let idxs: Vec<usize> = (0..per_part)
                .map(|_| rng.below(per_part as u64) as usize)
                .collect();
            let qd = 128usize;
            let start = Instant::now();
            let mut done = 0;
            while done < per_part {
                let this = qd.min(per_part - done);
                let refs: Vec<&[u8]> = idxs[done..done + this]
                    .iter()
                    .map(|&i| keys[i].as_bytes())
                    .collect();
                let got = vals_rt::block_on(e.multi_get_async(&refs), || {
                    e.device().poll();
                })
                .unwrap();
                debug_assert!(got.iter().all(|g| g.is_some()));
                done += this;
            }
            start.elapsed().as_secs_f64()
        },
    );
}

pub(super) fn poll_until(timeout: Duration, tick: Duration, mut f: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if f() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(tick);
    }
}

pub(super) fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(default)
}

#[cfg(feature = "spdk")]
pub(super) fn workload_engineasync(pci: &str, n: usize, vsize: usize, batch: usize) {
    use vals_spdk::spdk::{SpdkBlockDevice, SpdkConfig};

    let dev = SpdkBlockDevice::open(&SpdkConfig {
        pci_addr: pci.to_string(),
        nsid: 1,
    })
    .expect("open spdk nvme");
    let cfg = bench_config(Ordering::Unsorted);
    let all_keys: Vec<String> = (0..n).map(|i| format!("key-{i:012}")).collect();
    let val = make_value(vsize, 0);
    let mut e = Engine::create(dev, cfg).unwrap();
    for k in &all_keys {
        e.put(k.as_bytes(), &val, 0).unwrap();
    }
    e.sync().unwrap();

    let mut rng = Rng(0x6A09_E667_F3BC_C909);
    let idxs: Vec<usize> = (0..n).map(|_| rng.below(n as u64) as usize).collect();
    let mut lat = Vec::with_capacity(n);
    let start = Instant::now();
    let mut done = 0;
    while done < n {
        let this = batch.min(n - done);
        let refs: Vec<&[u8]> = idxs[done..done + this]
            .iter()
            .map(|&i| all_keys[i].as_bytes())
            .collect();
        let t = Instant::now();
        let got = vals_rt::block_on(e.multi_get_async(&refs), || {
            e.device().poll();
        })
        .unwrap();
        let per_key = t.elapsed().as_nanos() as u64 / this as u64;
        for _ in 0..this {
            lat.push(per_key);
        }
        debug_assert!(got.iter().all(|g| g.is_some()));
        done += this;
    }
    report(
        &format!("engineasync (batch={batch})"),
        lat,
        start.elapsed().as_secs_f64(),
        (n * vsize) as u64,
    );
}

