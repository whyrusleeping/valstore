use super::*;
use super::keyfilter::process_rss;

fn key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

fn scans(e: &mut Engine<FileBlockDevice>) -> (f64, u64) {
    let started = std::time::Instant::now();
    let mut bytes = 0;
    while let Some(mut plan) = e.cover_scan_plan() {
        let dev = e.device().sibling().unwrap();
        while !plan.step(&dev, 16 << 20).unwrap() {}
        bytes += plan.bytes_read();
        assert!(e.cover_scan_commit(plan));
    }
    (started.elapsed().as_secs_f64(), bytes)
}

fn checkpoint(e: &Engine<FileBlockDevice>, stored: u64, rounds: u64, batch: u64, label: &str) {
    let probes = 200_000u64;
    let before = e.key_filter_stats();
    let blooms = crate::attribution::lookup_snapshot()[3];
    let mut fp = 0;
    let mut blocks = 0;
    for i in 0..probes {
        let id = (crate::run::splitmix64(i) % (rounds * batch)) * 2 + 1;
        let k = key(id);
        fp += e.key_filter.may_contain(&k) as u64;
        let probe = e.probe_version(&k);
        let (version, read) = vals_rt::block_on(Engine::version_from_probe_counted(probe, e.device()), || {}).unwrap();
        assert!(version.is_none());
        blocks += read;
    }
    let after = e.key_filter_stats();
    let after_blooms = crate::attribution::lookup_snapshot()[3];
    eprintln!("KF396 checkpoint={label} stored={stored} runs={} capacity={} inserted={} subs={} bytes={} resident={} bpk={:.6} rss={} rebuilding={} rebuild_peak={} started={} completed={} resets={} est_fpr={:.6} measured_fpr={:.6} run_probes={:.6} bloom_positives={:.6} blocks={:.6}",
        e.run_count(), after.capacity_keys, after.inserted_keys, after.sub_filters, after.filter_bytes, after.resident_bytes,
        after.resident_bytes as f64 / stored as f64, process_rss(), after.rebuilding, after.rebuild_peak_bytes,
        after.rebuilds_started, after.rebuilds_completed, after.resets, after.estimated_fpr, fp as f64 / probes as f64,
        (after.run_probes - before.run_probes) as f64 / probes as f64,
        (after_blooms[2] - blooms[2]) as f64 / probes as f64, blocks as f64 / probes as f64);
}

#[test]
#[ignore]
fn keyfilter_file_ingest_measurement() {
    let total = std::env::var("VALS_KF_MEASURE_KEYS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(100_000_000);
    let batch = std::env::var("VALS_KF_MEASURE_BATCH").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(1_000_000).min(total);
    assert_eq!(total % batch, 0);
    let rounds = total / batch;
    let path = temp_path("kf396-file-measure");
    let dev = FileBlockDevice::create(&path, 4096, (total * 512 / 4096 + 8192) * 3).unwrap();
    let mut e = Engine::create_prewiped(dev, Config { seg_blocks: 256, reserved_blocks: 0, ordering: Ordering::Unsorted });
    e.key_filter.set_params(crate::keyfilter::KeyFilterParams { min_keys: (total / 100).max(1), ..crate::keyfilter::KeyFilterParams::from_env() });
    let mut put_seconds = 0.0;
    let mut flush_seconds = 0.0;
    let mut merge_seconds = 0.0;
    let mut scan_seconds = 0.0;
    let mut scan_bytes = 0;
    let mut requests = 0u64;
    let mut merges = 0;
    let mut previous_subs = 0;
    for round in 0..rounds {
        let started = std::time::Instant::now();
        for i in 0..batch {
            let k = key((i * rounds + round) * 2);
            let probe = e.probe_version(&k);
            assert!(vals_rt::block_on(Engine::version_from_probe(probe, e.device()), || {}).unwrap().is_none());
            e.put_kind_authoritative(&k, b"value", (round + 1) as u128, false).unwrap();
            requests += 1;
            if round > 0 && i % 100 == 0 {
                let old = key((i * rounds + round - 1) * 2);
                e.put_kind(&old, b"updated", (round + 1) as u128, i % 1000 == 0).unwrap();
                requests += 1;
            }
        }
        put_seconds += started.elapsed().as_secs_f64();
        let started = std::time::Instant::now();
        run_flush(&mut e, RunBuildOpts::default()).unwrap();
        flush_seconds += started.elapsed().as_secs_f64();
        let started = std::time::Instant::now();
        if let Some(plan) = e.compact_plan(RunBuildOpts::default(), None).unwrap() {
            let wdev = Arc::new(e.device().sibling().unwrap());
            let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap_or_else(|(_, err)| panic!("{err}"));
            e.compact_commit(written).unwrap();
            merges += 1;
        }
        merge_seconds += started.elapsed().as_secs_f64();
        let stats = e.key_filter_stats();
        if stats.sub_filters != previous_subs || stats.rebuilding != 0 {
            eprintln!("KF396 transition stored={} capacity={} inserted={} subs={} bytes={} resident={} rebuilding={} peak={} resets={}",
                (round + 1) * batch, stats.capacity_keys, stats.inserted_keys, stats.sub_filters, stats.filter_bytes,
                stats.resident_bytes, stats.rebuilding, stats.rebuild_peak_bytes, stats.resets);
        }
        previous_subs = stats.sub_filters;
        let (seconds, bytes) = scans(&mut e);
        scan_seconds += seconds;
        scan_bytes += bytes;
        if (round + 1) % (rounds / 10).max(1) == 0 || round + 1 == rounds {
            checkpoint(&e, (round + 1) * batch, rounds, batch, "steady");
            eprintln!("KF396 timing stored={} requests={requests} put_s={put_seconds:.6} flush_s={flush_seconds:.6} merge_s={merge_seconds:.6} scan_s={scan_seconds:.6} scan_bytes={scan_bytes} merges={merges} puts_per_s={:.3} ingest_per_s={:.3}",
                (round + 1) * batch, requests as f64 / put_seconds,
                requests as f64 / (put_seconds + flush_seconds + merge_seconds));
        }
    }
    e.sync().unwrap();
    drop(e);
    std::fs::remove_file(path).unwrap();
}
