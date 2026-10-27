use super::*;

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

fn policy(cap: u64, force_runs: usize) -> MergePolicy {
    MergePolicy {
        k: 6,
        min_k: 4,
        size_ratio: 1.5,
        size_cap: cap,
        force_runs,
        freeze_bytes: cap,
    }
}

fn pre_0381_select(sizes: &[u64], p: &MergePolicy) -> Option<MergeWindow> {
    let n = sizes.len();
    if n < 2 {
        return None;
    }
    let start = select_merge_suffix(sizes, p.k, p.size_ratio, p.size_cap);
    if n - start >= p.min_k {
        return Some(MergeWindow { start, end: n, forced: false });
    }
    if n >= 2 * p.k {
        let forced = select_merge_suffix(sizes, p.k, f64::INFINITY, p.size_cap);
        return Some(MergeWindow { start: forced.min(n - 2), end: n, forced: true });
    }
    None
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 11
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

#[derive(Default)]
struct Sim {
    runs: Vec<u64>,
    flushed: u64,
    rewritten: u64,
    merges: u64,
    forced: u64,
    max_input: u64,
    max_runs: usize,
}

impl Sim {
    fn flush(&mut self, bytes: u64, select: &dyn Fn(&[u64]) -> Option<MergeWindow>) {
        self.flush_checked(bytes, select, &mut |_, _| {});
    }

    fn flush_checked(
        &mut self,
        bytes: u64,
        select: &dyn Fn(&[u64]) -> Option<MergeWindow>,
        check: &mut dyn FnMut(&[u64], MergeWindow),
    ) {
        self.runs.push(bytes);
        self.flushed += bytes;
        while let Some(w) = select(&self.runs) {
            assert!(w.start + 2 <= w.end && w.end <= self.runs.len(), "{w:?} over {} runs", self.runs.len());
            check(&self.runs, w);
            let input: u64 = self.runs[w.start..w.end].iter().sum();
            self.rewritten += input;
            self.merges += 1;
            self.forced += u64::from(w.forced);
            self.max_input = self.max_input.max(input);
            self.runs.splice(w.start..w.end, [input]);
        }
        self.max_runs = self.max_runs.max(self.runs.len());
    }

    fn write_amp(&self) -> f64 {
        self.rewritten as f64 / self.flushed as f64
    }
}

fn flush_sizes(seed: u64, mean: u64, n: usize) -> Vec<u64> {
    let mut rng = Rng(seed.wrapping_mul(0x9e3779b97f4a7c15) | 1);
    (0..n).map(|_| mean / 2 + rng.below(mean)).collect()
}

fn amplification_bound(p: &MergePolicy, min_flush: u64) -> f64 {
    (p.size_cap as f64 / min_flush as f64).ln() / (1.0 + 1.0 / p.size_ratio).ln() + 1.0
}

#[test]
fn recurring_and_forced_merges_never_exceed_the_cap_or_take_frozen_runs() {
    let (mut merging_seeds, mut forced_merges) = (0u32, 0u64);
    for seed in 0..96u64 {
        let mut rng = Rng(seed + 1);
        let cap = (64 * MIB) << rng.below(7);
        let k = 2 + rng.below(7) as usize;
        let p = MergePolicy {
            k,
            min_k: 2 + rng.below(k as u64 - 1) as usize,
            size_ratio: [1.0, 1.5, 2.0, 4.0][rng.below(4) as usize],
            size_cap: cap,
            force_runs: 2 + rng.below(15) as usize,
            freeze_bytes: if rng.below(3) == 0 { cap / 4 + rng.below(cap / 2) } else { cap },
        };
        let mean = cap / (2 + rng.below(40));
        let shape = rng.below(3);
        let mut sim = Sim::default();
        let mut merges = 0u64;
        for _ in 0..400 {
            let bytes = match shape {
                0 => mean / 2 + rng.below(mean),
                1 => (mean >> 6).max(1) << rng.below(8),
                _ if rng.below(50) == 0 => cap + rng.below(4 * cap),
                _ => mean / 2 + rng.below(mean),
            };
            let select = |s: &[u64]| select_recurring_merge(s, &p);
            sim.flush_checked(bytes, &select, &mut |runs, w| {
                let inputs = &runs[w.start..w.end];
                let total: u64 = inputs.iter().sum();
                assert!(total <= cap, "seed {seed}: {w:?} input {total} > cap {cap} ({inputs:?})");
                assert!(w.end - w.start <= p.k, "seed {seed}: fan-in over k {w:?}");
                assert!(
                    inputs.iter().all(|&r| !p.frozen(r)),
                    "seed {seed}: frozen input in {w:?}: {inputs:?} (freeze {})",
                    p.freeze_bytes
                );
                if !w.forced {
                    assert_eq!(w.end, runs.len(), "seed {seed}: tiered merges take the newest suffix");
                }
                merges += 1;
            });
        }
        assert!(sim.max_input <= cap, "seed {seed}");
        merging_seeds += u32::from(merges > 0);
        forced_merges += sim.forced;
    }
    assert!(merging_seeds >= 80, "only {merging_seeds}/96 random cases merged at all");
    assert!(forced_merges > 0, "the valve never fired across the random cases");
}

#[test]
fn creates_only_rewrite_amplification_is_bounded_and_flat_in_data() {
    let p = policy(2 * GIB, MERGE_FORCE_RUNS);
    let flush = 200 * MIB;
    let bound = amplification_bound(&p, flush / 2);
    let graduated = p.size_cap as f64 * p.size_ratio / (1.0 + p.size_ratio);
    for seed in 0..8u64 {
        let sizes = flush_sizes(seed, flush, 1600);
        let select = |s: &[u64]| select_recurring_merge(s, &p);
        let mut sim = Sim::default();
        let mut checkpoints = Vec::new();
        for (i, &b) in sizes.iter().enumerate() {
            sim.flush(b, &select);
            if [400, 800, 1600].contains(&(i + 1)) {
                checkpoints.push((sim.flushed, sim.write_amp(), sim.runs.len()));
            }
        }
        for &(data, wa, runs) in &checkpoints {
            assert!(wa <= bound, "seed {seed}: rewrite amplification {wa:.2} over the log bound {bound:.2}");
            let floor = data.div_ceil(p.size_cap) as usize;
            let ceiling = (2.0 * data as f64 / graduated) as usize + p.force_runs + p.k;
            assert!(
                (floor..=ceiling).contains(&runs),
                "seed {seed}: {runs} runs for {data} bytes outside {floor}..={ceiling}"
            );
        }
        let (wa1, wa4) = (checkpoints[0].1, checkpoints[2].1);
        assert!(wa4 <= wa1 * 1.1 + 0.05, "seed {seed}: amplification grows with data: {wa1:.2} → {wa4:.2}");
        let growth = checkpoints[2].2 as f64 / checkpoints[1].2 as f64;
        assert!((1.6..=2.4).contains(&growth), "seed {seed}: run count not ~linear in data: {checkpoints:?}");
        assert!(sim.max_input <= p.size_cap);
    }
}

#[test]
fn knockout_pre_0381_forced_rule_exceeds_the_cap_and_amplification_grows_with_data() {
    let p = policy(2 * GIB, MERGE_FORCE_RUNS);
    let flush = 200 * MIB;
    let bound = amplification_bound(&p, flush / 2);
    let sizes = flush_sizes(3, flush, 1600);
    let select = |s: &[u64]| pre_0381_select(s, &p);
    let mut sim = Sim::default();
    let mut wa = Vec::new();
    for (i, &b) in sizes.iter().enumerate() {
        sim.flush(b, &select);
        if [800, 1600].contains(&(i + 1)) {
            wa.push(sim.write_amp());
        }
    }
    assert!(sim.max_input > 10 * p.size_cap, "old rule merge input {} stays under 10× cap", sim.max_input);
    assert!(wa[1] > 1.8 * wa[0], "old rule amplification must grow ~linearly with data: {wa:?}");
    assert!(wa[1] > 10.0 * bound, "old rule amplification {:.1} not ≫ bound {bound:.1}", wa[1]);
}

#[test]
fn valve_folds_an_active_tail_that_tiering_cannot_fold() {
    let p = policy(2 * GIB, MERGE_FORCE_RUNS);
    let sizes: Vec<u64> = (0..300).flat_map(|_| (0..6).rev().map(|i| (MIB / 2) << (2 * i))).collect();
    let run = |p: MergePolicy| {
        let mut sim = Sim::default();
        let mut max_tail = 0usize;
        for &b in &sizes {
            sim.flush(b, &|s: &[u64]| select_recurring_merge(s, &p));
            max_tail = max_tail.max(sim.runs.len() - p.active_start(&sim.runs));
        }
        (sim, max_tail)
    };
    let (valve, valve_tail) = run(p);
    let (none, none_tail) = run(MergePolicy { force_runs: usize::MAX, ..p });
    println!(
        "valve: runs={} tail={valve_tail} wa={:.2} forced={}; none: runs={} tail={none_tail} wa={:.2}",
        valve.runs.len(),
        valve.write_amp(),
        valve.forced,
        none.runs.len(),
        none.write_amp()
    );
    assert!(valve.forced > 0, "the valve must fire on a geometric flush cadence");
    assert!(
        valve.runs.len() * 2 < none.runs.len(),
        "valve runs {} vs {} without",
        valve.runs.len(),
        none.runs.len()
    );
    assert!(valve_tail <= none_tail, "active tail {valve_tail} vs {none_tail} without the valve");
    assert!(valve.max_input <= p.size_cap);
    assert!(valve.write_amp() <= amplification_bound(&p, MIB / 2), "valve WA {:.2}", valve.write_amp());
}

#[test]
fn forced_window_is_the_cheapest_adjacent_balanced_window_in_the_active_tail() {
    let p = MergePolicy {
        k: 6,
        min_k: 4,
        size_ratio: 1.5,
        size_cap: 1000,
        force_runs: 3,
        freeze_bytes: 1000,
    };
    let forced = |start, end| Some(MergeWindow { start, end, forced: true });
    assert_eq!(select_recurring_merge(&[100, 100], &p), None, "below the valve");
    assert_eq!(
        select_recurring_merge(&[100, 100, 400], &p),
        forced(0, 2),
        "a balanced older pair beats feeding small runs into a bigger newer one"
    );
    assert_eq!(
        select_recurring_merge(&[100, 100, 100], &p),
        forced(0, 3),
        "lowest bytes per eliminated run wins"
    );
    assert_eq!(
        select_recurring_merge(&[900, 100, 100, 100], &p),
        forced(1, 4),
        "a run too large for any balanced capped window is frozen and never an input"
    );
    assert_eq!(
        select_recurring_merge(&[500, 550, 500], &p),
        None,
        "no adjacent window fits under the cap: let the run count grow"
    );
    assert_eq!(
        select_recurring_merge(&[300, 10, 100], &p),
        forced(1, 3),
        "with no balanced window the cheapest capped window is taken"
    );
    let frozen_tail = MergePolicy { freeze_bytes: 200, ..p };
    assert_eq!(
        select_recurring_merge(&[100, 100, 100, 250, 100, 100], &frozen_tail),
        None,
        "the valve only counts and folds the active tail after the newest frozen run"
    );
    assert_eq!(
        select_recurring_merge(&[100, 100, 100, 250, 100, 100, 100], &frozen_tail),
        forced(4, 7)
    );
}

#[test]
#[ignore]
fn merge_policy_micro_simulation() {
    let cap = 2 * GIB;
    let flush = 200 * MIB;
    let p = policy(cap, MERGE_FORCE_RUNS);
    let sizes = flush_sizes(7, flush, 768);
    let rules: [(&str, &dyn Fn(&[u64]) -> Option<MergeWindow>); 2] = [
        ("pre-0381", &|s: &[u64]| pre_0381_select(s, &p)),
        ("0381", &|s: &[u64]| select_recurring_merge(s, &p)),
    ];
    println!("flush ~{} MiB (uniform ±50%), cap {} GiB, K={} min_k={} ratio={} force_runs={}",
        flush / MIB, cap / GIB, p.k, p.min_k, p.size_ratio, p.force_runs);
    println!("{:>9} {:>9} {:>11} {:>6} {:>12} {:>8} {:>7}", "policy", "data_GB", "merge_in/fl", "runs", "max_in_GiB", "merges", "forced");
    for (name, select) in rules {
        let mut sim = Sim::default();
        for (i, &b) in sizes.iter().enumerate() {
            sim.flush(b, select);
            if (i + 1) % 48 == 0 {
                println!(
                    "{name:>9} {:>9.1} {:>11.2} {:>6} {:>12.2} {:>8} {:>7}",
                    sim.flushed as f64 / 1e9,
                    sim.write_amp(),
                    sim.runs.len(),
                    sim.max_input as f64 / GIB as f64,
                    sim.merges,
                    sim.forced
                );
            }
        }
    }
}
