use vals_client::hlc::COUNTER_BITS;
use vals_cluster::antientropy::{leaf_digests_versioned, DEFAULT_BUCKETS};
use vals_cluster::replica::VersionedStore;
use vals_cluster::{observe, Version};
use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine, Ordering};

const SOURCE_FOLD_BUDGET: u64 = 4096;

fn version(stamp: u64, node: u32) -> Version {
    Version {
        hlc: stamp << COUNTER_BITS,
        node: node.into(),
    }
}

#[test]
fn cooperative_seed_source_folds_are_bounded_per_turn() {
    let mem = MemBlockDevice::new(vals_spdk::DEFAULT_BLOCK_SIZE, 1 << 20);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut source = VersionedStore::new(Engine::create_prewiped(mem.share(), cfg), 7).unwrap();
    for run in 0..2u64 {
        for i in 0..20_000u64 {
            let key = format!("run-{run:02}-{i:05}");
            source
                .apply(key.as_bytes(), Some(b"v"), version(1_000 + run * 20_000 + i, 7))
                .unwrap();
        }
        source.flush().unwrap();
    }
    source.sync().unwrap();
    assert!(source.engine().run_count() >= 2);
    drop(source);

    let mut store = VersionedStore::open(mem.share(), cfg, 7).unwrap();
    for i in 0..(SOURCE_FOLD_BUDGET * 2 + 17) {
        let key = format!("pending-{i:05}");
        store
            .apply(key.as_bytes(), Some(b"v"), version(100_000 + i, 7))
            .unwrap();
    }

    let mut steps = 0usize;
    let mut fold_steps = 0usize;
    loop {
        let before = observe::snapshot().ae_seed_fold_ops;
        let done = store.seed_step().unwrap();
        let folded = observe::snapshot().ae_seed_fold_ops - before;
        assert!(folded <= SOURCE_FOLD_BUDGET, "stream turn emitted {folded} winners");
        fold_steps += usize::from(folded != 0);
        steps += 1;
        assert!(steps < 10_000, "stream seed failed to terminate");
        if done { break; }
    }
    assert!(fold_steps > 6);

    let live: Vec<(Vec<u8>, Version)> = store
        .engine()
        .recovered_versions()
        .unwrap()
        .into_iter()
        .map(|(key, raw)| {
            (
                key.into_vec(),
                Version {
                    hlc: (raw >> 64) as u64,
                    node: raw as u64,
                },
            )
        })
        .collect();
    let expected = leaf_digests_versioned(
        live.iter().map(|(key, version)| (&key[..], *version)),
        DEFAULT_BUCKETS,
    );
    assert_eq!(store.version_leaf_digests(DEFAULT_BUCKETS).unwrap(), expected);
}
