use vals_spdk::{BlockDevice, MemBlockDevice};
use vals_store::{vlog_replay_skip_totals, Config, Engine, Ordering};

const BLOCK_SIZE: usize = 512;
const NUM_BLOCKS: u64 = 4096;
const KEYS: u32 = 40;

fn cfg() -> Config {
    Config {
        seg_blocks: 64,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    }
}

fn key(i: u32) -> Vec<u8> {
    format!("key-{i:03}").into_bytes()
}

fn val(i: u32) -> Vec<u8> {
    format!("value-{i:03}-{}", "x".repeat(90)).into_bytes()
}

fn write_synced_keys() -> MemBlockDevice {
    let dev = MemBlockDevice::new(BLOCK_SIZE, NUM_BLOCKS);
    let mut e = Engine::create_prewiped(dev.share(), cfg());
    for i in 0..KEYS {
        e.put(&key(i), &val(i), i as u128 + 1).unwrap();
    }
    e.sync().unwrap();
    dev
}

fn flip_bit_in_key(dev: &MemBlockDevice, k: &[u8]) {
    let mut block = vec![0u8; BLOCK_SIZE];
    for lba in 0..NUM_BLOCKS {
        dev.read_at(lba, &mut block).unwrap();
        if let Some(pos) = block.windows(k.len()).position(|w| w == k) {
            block[pos + k.len() - 1] ^= 0x01;
            dev.write_at(lba, &block).unwrap();
            dev.flush().unwrap();
            return;
        }
    }
    panic!("key {:?} not found on the device", String::from_utf8_lossy(k));
}

fn present(e: &Engine<MemBlockDevice>) -> Vec<u32> {
    (0..KEYS)
        .filter(|&i| {
            let got = e.get(&key(i)).unwrap();
            if let Some(v) = &got {
                assert_eq!(v.as_slice(), val(i).as_slice(), "key {i} recovered with a wrong value");
            }
            got.is_some()
        })
        .collect()
}

#[test]
fn replay_recovers_synced_records_after_mid_segment_corruption_and_counts_it() {
    let dev = write_synced_keys();
    flip_bit_in_key(&dev, &key(0));
    let (skips_before, bytes_before) = vlog_replay_skip_totals();
    let e = Engine::open(dev.share(), cfg()).unwrap();
    let (skips_after, bytes_after) = vlog_replay_skip_totals();
    assert_eq!(present(&e), (1..KEYS).collect::<Vec<_>>(), "every synced record after the damaged one must replay");
    assert_eq!(skips_after - skips_before, 1, "the damaged region must be counted");
    assert!(bytes_after > bytes_before);

    let dev = write_synced_keys();
    flip_bit_in_key(&dev, &key(KEYS - 1));
    let (skips_before, _) = vlog_replay_skip_totals();
    let e = Engine::open(dev.share(), cfg()).unwrap();
    assert_eq!(present(&e), (0..KEYS - 1).collect::<Vec<_>>(), "a torn tail drops only the damaged last record");
    assert_eq!(vlog_replay_skip_totals().0, skips_before, "a torn tail is not counted as corruption");
}
