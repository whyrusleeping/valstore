use super::*;

pub(super) fn bloom_hash(key: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[derive(Clone, Copy, Debug)]
pub struct BloomKey {
    h1: u64,
    z: u64,
}

impl BloomKey {
    #[inline]
    pub fn new(key: &[u8]) -> BloomKey {
        let h1 = bloom_hash(key);
        BloomKey { h1, z: splitmix64(h1) }
    }

    #[inline]
    pub(crate) fn z(&self) -> u64 {
        self.z
    }
}

#[inline]
pub(crate) fn prefetch_read<T>(ptr: *const T) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: prefetch is only a non-faulting cache hint; callers derive pointers from live vectors.
    unsafe {
        std::arch::x86_64::_mm_prefetch(ptr.cast(), std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = ptr;
}

pub(crate) fn splitmix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bloom {
    pub(super) num_bits: u64,
    pub(super) k: u32,
    pub(super) bits: Vec<u64>,
}

impl Bloom {
    fn estimated_fpr(&self) -> f64 {
        let ones: u64 = self.bits.iter().map(|w| w.count_ones() as u64).sum();
        (ones as f64 / self.num_bits.max(1) as f64).powi(self.k as i32)
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn with_capacity(n_upper: usize, bits_per_key: usize) -> Bloom {
        let num_bits = ((n_upper.max(1) * bits_per_key) as u64)
            .next_multiple_of(64)
            .max(64);
        let k = ((bits_per_key as f64 * std::f64::consts::LN_2).round() as u32).clamp(1, 30);
        Bloom {
            num_bits,
            k,
            bits: vec![0u64; (num_bits / 64) as usize],
        }
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn build<'a>(keys: impl Iterator<Item = &'a [u8]>, n: usize, bits_per_key: usize) -> Bloom {
        let mut bloom = Bloom::with_capacity(n, bits_per_key);
        for key in keys {
            bloom.insert(key);
        }
        bloom
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    fn positions(&self, key: &[u8]) -> impl Iterator<Item = u64> + '_ {
        self.positions_hashed(&BloomKey::new(key))
    }

    fn positions_hashed(&self, hk: &BloomKey) -> impl Iterator<Item = u64> + '_ {
        let h1 = hk.h1;
        let h2 = hk.z | 1;
        let num_bits = self.num_bits;
        (0..self.k).map(move |i| h1.wrapping_add((i as u64).wrapping_mul(h2)) % num_bits)
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    fn insert(&mut self, key: &[u8]) {
        let positions: Vec<u64> = self.positions(key).collect();
        for p in positions {
            self.bits[(p / 64) as usize] |= 1u64 << (p % 64);
        }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.contains_hashed(&BloomKey::new(key))
    }

    pub fn contains_hashed(&self, hk: &BloomKey) -> bool {
        self.positions_hashed(hk)
            .all(|p| self.bits[(p / 64) as usize] & (1u64 << (p % 64)) != 0)
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(12 + self.bits.len() * 8 + 4);
        b.extend_from_slice(&self.num_bits.to_le_bytes());
        b.extend_from_slice(&self.k.to_le_bytes());
        for w in &self.bits {
            b.extend_from_slice(&w.to_le_bytes());
        }
        let crc = crc::crc32(&b);
        b.extend_from_slice(&crc.to_le_bytes());
        b
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Bloom, &'static str> {
        let body = crc_checked_body(bytes)?;
        let mut c = Cursor { b: body, off: 0 };
        let num_bits = c.u64()?;
        let k = c.u32()?;
        if num_bits == 0 || num_bits % 64 != 0 {
            return Err("bad bloom size");
        }
        let words = num_bits / 64;
        let available = body.len() - c.off;
        if words > (available / 8) as u64 {
            return Err("truncated");
        }
        if words < (available / 8) as u64 || !available.is_multiple_of(8) {
            return Err("trailing bytes");
        }
        let mut bits = Vec::with_capacity(words as usize);
        for _ in 0..words {
            bits.push(c.u64()?);
        }
        Ok(Bloom { num_bits, k, bits })
    }
}

const BLOCKED_BLOOM_MAGIC: u32 = 0x3146_4242;

pub(super) const BLOOM_BLOCK_BITS: u64 = 512;
const BLOOM_BLOCK_WORDS: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockedBloom {
    pub(super) num_bits: u64,
    pub(super) k: u32,
    pub(super) bits: Vec<u64>,
}

impl BlockedBloom {
    fn estimated_fpr(&self) -> f64 {
        let blocks = self.bits.chunks(BLOOM_BLOCK_WORDS);
        let n = blocks.len().max(1);
        blocks
            .map(|b| {
                let ones: u32 = b.iter().map(|w| w.count_ones()).sum();
                (ones as f64 / BLOOM_BLOCK_BITS as f64).powi(self.k as i32)
            })
            .sum::<f64>()
            / n as f64
    }

    pub fn with_capacity(n_upper: usize, bits_per_key: usize) -> BlockedBloom {
        let num_bits = ((n_upper.max(1) * bits_per_key) as u64)
            .next_multiple_of(BLOOM_BLOCK_BITS)
            .max(BLOOM_BLOCK_BITS);
        let k = ((bits_per_key as f64 * std::f64::consts::LN_2).round() as u32).clamp(1, 30);
        BlockedBloom {
            num_bits,
            k,
            bits: vec![0u64; (num_bits / 64) as usize],
        }
    }

    pub fn build<'a>(
        keys: impl Iterator<Item = &'a [u8]>,
        n: usize,
        bits_per_key: usize,
    ) -> BlockedBloom {
        let mut bloom = BlockedBloom::with_capacity(n, bits_per_key);
        for key in keys {
            bloom.insert(key);
        }
        bloom
    }

    #[inline]
    fn block_base_and_hashes(&self, key: &[u8]) -> (usize, u64, u64) {
        self.block_base_and_hashes_z(BloomKey::new(key).z)
    }

    #[inline]
    fn block_base_and_hashes_z(&self, z: u64) -> (usize, u64, u64) {
        let num_blocks = self.num_bits / BLOOM_BLOCK_BITS;
        let block = ((z as u128 * num_blocks as u128) >> 64) as usize;
        let z2 = splitmix64(z);
        (block * BLOOM_BLOCK_WORDS, z2, (z2 >> 32) | 1)
    }

    pub(super) fn insert(&mut self, key: &[u8]) {
        let (base, h1, h2) = self.block_base_and_hashes(key);
        for i in 0..self.k as u64 {
            let pos = h1.wrapping_add(i.wrapping_mul(h2)) % BLOOM_BLOCK_BITS;
            self.bits[base + (pos / 64) as usize] |= 1u64 << (pos % 64);
        }
    }

    #[inline]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.contains_hashed(&BloomKey::new(key))
    }

    #[inline]
    fn prefetch(&self, hk: &BloomKey) {
        let num_blocks = self.num_bits / BLOOM_BLOCK_BITS;
        let block = ((hk.z as u128 * num_blocks as u128) >> 64) as usize;
        prefetch_read(self.bits.as_ptr().wrapping_add(block * BLOOM_BLOCK_WORDS));
    }

    #[inline]
    pub fn contains_hashed(&self, hk: &BloomKey) -> bool {
        let (base, h1, h2) = self.block_base_and_hashes_z(hk.z);
        (0..self.k as u64).all(|i| {
            let pos = h1.wrapping_add(i.wrapping_mul(h2)) % BLOOM_BLOCK_BITS;
            self.bits[base + (pos / 64) as usize] & (1u64 << (pos % 64)) != 0
        })
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(16 + self.bits.len() * 8 + 4);
        b.extend_from_slice(&BLOCKED_BLOOM_MAGIC.to_le_bytes());
        b.extend_from_slice(&self.num_bits.to_le_bytes());
        b.extend_from_slice(&self.k.to_le_bytes());
        for w in &self.bits {
            b.extend_from_slice(&w.to_le_bytes());
        }
        let crc = crc::crc32(&b);
        b.extend_from_slice(&crc.to_le_bytes());
        b
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<BlockedBloom, &'static str> {
        let body = crc_checked_body(bytes)?;
        let mut c = Cursor { b: body, off: 0 };
        if c.u32()? != BLOCKED_BLOOM_MAGIC {
            return Err("bad blocked-bloom magic");
        }
        let num_bits = c.u64()?;
        if num_bits == 0 || num_bits % BLOOM_BLOCK_BITS != 0 {
            return Err("bad blocked-bloom size");
        }
        let k = c.u32()?;
        let words = num_bits / 64;
        let available = body.len() - c.off;
        if words > (available / 8) as u64 {
            return Err("truncated");
        }
        if words < (available / 8) as u64 || !available.is_multiple_of(8) {
            return Err("trailing bytes");
        }
        let mut bits = Vec::with_capacity(words as usize);
        for _ in 0..words {
            bits.push(c.u64()?);
        }
        Ok(BlockedBloom { num_bits, k, bits })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunFilter {
    Classic(Bloom),
    Blocked(BlockedBloom),
    Unfiltered,
}

impl RunFilter {
    pub(crate) fn estimated_fpr(&self) -> f64 {
        match self {
            RunFilter::Classic(b) => b.estimated_fpr(),
            RunFilter::Blocked(b) => b.estimated_fpr(),
            RunFilter::Unfiltered => 1.0,
        }
    }

    pub(super) fn decode(version: u16, bytes: &[u8]) -> Result<RunFilter, &'static str> {
        if version >= 4 {
            Ok(RunFilter::Blocked(BlockedBloom::decode(bytes)?))
        } else {
            Ok(RunFilter::Classic(Bloom::decode(bytes)?))
        }
    }

    #[inline]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.contains_hashed(&BloomKey::new(key))
    }

    #[inline]
    pub fn contains_hashed(&self, hk: &BloomKey) -> bool {
        match self {
            RunFilter::Classic(b) => b.contains_hashed(hk),
            RunFilter::Blocked(b) => b.contains_hashed(hk),
            RunFilter::Unfiltered => true,
        }
    }

    #[inline]
    pub fn prefetch(&self, hk: &BloomKey) {
        if let RunFilter::Blocked(b) = self {
            b.prefetch(hk);
        }
    }
}
