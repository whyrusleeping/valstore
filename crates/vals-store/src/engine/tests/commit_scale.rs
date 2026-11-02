use super::*;

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn small_merge_commit_us(scale: u64, memtable: u64, runs: u64, merges: u64) -> (u64, u64, u64, Vec<f64>) {
    let per_run = 4 * scale / runs.max(1);
    let blocks = 4 * scale + memtable / 64 + 4 * runs + 65_536;
    let mut e = Engine::create_prewiped(
        MemBlockDevice::new(4096, blocks),
        Config { seg_blocks: 1, reserved_blocks: 0, ordering: Ordering::Sorted },
    );
    let opts = RunBuildOpts::default();
    let value = [7u8; 1000];
    let mut version = 1u128;
    for r in 0..runs {
        for i in 0..per_run {
            e.put(format!("run-{r:05}-{i:08}").as_bytes(), &value, version).unwrap();
            version += 1;
        }
        run_flush(&mut e, opts).expect("flush");
    }
    for (m, r) in (runs..runs + 2 * merges).enumerate() {
        for i in 0..1_000u64 {
            e.put(format!("new-{r:05}-{i:08}").as_bytes(), &value, version).unwrap();
            version += 1;
        }
        if m % 2 == 1 {
            for i in 0..100u64 {
                e.put(format!("new-{:05}-{i:08}", r - 1).as_bytes(), &value, version).unwrap();
                version += 1;
            }
        }
        run_flush(&mut e, opts).expect("flush");
    }
    for i in 0..memtable {
        e.put(format!("mem-{i:012}").as_bytes(), b"v", version).unwrap();
        version += 1;
    }
    let mut commits = Vec::new();
    for _ in 0..merges {
        let n = e.run_count();
        let plan = e.compact_plan_window_for_test(opts, n - 2, n, None).unwrap().expect("plan");
        let wdev = Arc::new(e.device().sibling().unwrap());
        let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
        let started = std::time::Instant::now();
        e.compact_commit(written).unwrap();
        commits.push(started.elapsed().as_secs_f64() * 1e6);
    }
    let refs = e.runs.iter().map(|or| or.meta.value_segs.as_ref().map_or(0, |v| v.len())).sum::<usize>() as u64;
    (e.used_segments(), refs, e.run_count() as u64, commits)
}

#[test]
#[ignore]
fn small_merge_commit_cost_vs_unaffected_state() {
    let memtable = env_u64("VALS_COMMIT_SCALE_MEMTABLE", 1_000_000);
    let runs = env_u64("VALS_COMMIT_SCALE_RUNS", 100);
    let merges = env_u64("VALS_COMMIT_SCALE_MERGES", 5);
    for scale in std::env::var("VALS_COMMIT_SCALE_SEGS").unwrap_or_else(|_| "10000,40000,160000".into())
        .split(',').map(|s| s.parse::<u64>().unwrap())
    {
        let (used, refs, run_count, mut commits) = small_merge_commit_us(scale, memtable, runs, merges);
        commits.sort_by(f64::total_cmp);
        eprintln!("COMMIT559 value_segs_target={scale} memtable={memtable} runs={run_count} used_segments={used} run_value_seg_refs={refs} merges={merges} commit_us_median={:.0} commit_us_min={:.0} commit_us_max={:.0}",
            commits[commits.len() / 2], commits[0], commits[commits.len() - 1]);
    }
}
