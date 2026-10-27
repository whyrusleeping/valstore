use super::*;

pub fn build_run_v2(
    entries: &[SnapshotEntry],
    opts: RunBuildOpts,
    bucket_of: impl Fn(&[u8]) -> u32,
) -> Vec<u8> {
    let mut out = Vec::new();
    let mut index = FenceBuilder::default();
    let mut i = 0;
    while i < entries.len() {
        let start = i;
        let mut est = 0usize;
        while i < entries.len() && (i == start || est < opts.block_target_bytes) {
            est += entries[i].key.len() + 24 + 16;
            i += 1;
        }
        let block = encode_block(&entries[start..i]);
        index.push(&entries[start].key, out.len() as u64, block.len() as u32).unwrap();
        out.extend_from_slice(&block);
    }
    let bloom_off = out.len() as u64;
    let bloom = Bloom::build(
        entries.iter().map(|e| e.key.as_slice()),
        entries.len(),
        opts.bloom_bits_per_key,
    );
    let bloom_bytes = bloom.encode();
    out.extend_from_slice(&bloom_bytes);
    let index_off = out.len() as u64;
    let index = index.finish();
    let idx = encode_index_bytes(&index, None);
    out.extend_from_slice(&idx);
    let sidecar_off = out.len() as u64;
    let mut by_bucket: Vec<Vec<(&[u8], u128)>> = vec![Vec::new(); opts.num_buckets as usize];
    for e in entries {
        let b = (bucket_of(&e.key) % opts.num_buckets) as usize;
        by_bucket[b].push((&e.key, e.version));
    }
    let mut side = Vec::new();
    side.extend_from_slice(&opts.num_buckets.to_le_bytes());
    for bucket in &by_bucket {
        side.extend_from_slice(&(bucket.len() as u32).to_le_bytes());
        for (k, v) in bucket {
            side.extend_from_slice(&(k.len() as u32).to_le_bytes());
            side.extend_from_slice(k);
            side.extend_from_slice(&v.to_le_bytes());
        }
    }
    let side_crc = crc::crc32(&side);
    side.extend_from_slice(&side_crc.to_le_bytes());
    out.extend_from_slice(&side);
    let f = encode_footer(&FooterFields {
        entry_count: entries.len() as u64,
        num_blocks: index.len() as u64,
        index_off,
        index_len: idx.len() as u64,
        bloom_off,
        bloom_len: bloom_bytes.len() as u64,
        sidecar_off,
        sidecar_len: side.len() as u64,
        num_buckets: opts.num_buckets,
        version: 2,
    });
    out.extend_from_slice(&f);
    out
}
