use std::sync::OnceLock;

const POLY: u32 = 0x82F6_3B78;

fn tables() -> &'static [[u32; 256]; 8] {
    static TABLES: OnceLock<[[u32; 256]; 8]> = OnceLock::new();
    TABLES.get_or_init(|| {
        let mut t = [[0u32; 256]; 8];
        let mut i = 0usize;
        while i < 256 {
            let mut c = i as u32;
            let mut k = 0;
            while k < 8 {
                c = if c & 1 != 0 { POLY ^ (c >> 1) } else { c >> 1 };
                k += 1;
            }
            t[0][i] = c;
            i += 1;
        }
        let mut n = 1usize;
        while n < 8 {
            let mut j = 0usize;
            while j < 256 {
                let prev = t[n - 1][j];
                t[n][j] = (prev >> 8) ^ t[0][(prev & 0xFF) as usize];
                j += 1;
            }
            n += 1;
        }
        t
    })
}

#[cfg(target_arch = "x86_64")]
fn has_sse42() -> bool {
    static OK: OnceLock<bool> = OnceLock::new();
    *OK.get_or_init(|| std::arch::is_x86_feature_detected!("sse4.2"))
}

const LANE: usize = 1024;

fn zshift_tables() -> &'static [[u32; 256]; 4] {
    static ZSHIFT: OnceLock<[[u32; 256]; 4]> = OnceLock::new();
    ZSHIFT.get_or_init(|| {
        let t0 = &tables()[0];
        let mut basis = [0u32; 32];
        for (i, b) in basis.iter_mut().enumerate() {
            let mut s = 1u32 << i;
            for _ in 0..LANE {
                s = (s >> 8) ^ t0[(s & 0xFF) as usize];
            }
            *b = s;
        }
        let mut t = [[0u32; 256]; 4];
        for (j, tj) in t.iter_mut().enumerate() {
            for (v, out) in tj.iter_mut().enumerate() {
                let mut acc = 0u32;
                for bit in 0..8 {
                    if v & (1 << bit) != 0 {
                        acc ^= basis[8 * j + bit];
                    }
                }
                *out = acc;
            }
        }
        t
    })
}

#[inline]
fn zshift(t: &[[u32; 256]; 4], s: u32) -> u32 {
    t[0][(s & 0xFF) as usize]
        ^ t[1][((s >> 8) & 0xFF) as usize]
        ^ t[2][((s >> 16) & 0xFF) as usize]
        ^ t[3][(s >> 24) as usize]
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn update_hw(state: u32, data: &[u8]) -> u32 {
    use std::arch::x86_64::{_mm_crc32_u64, _mm_crc32_u8};
    let mut s = state;
    let mut data = data;
    if data.len() >= 3 * LANE {
        let zt = zshift_tables();
        while data.len() >= 3 * LANE {
            let (blk, rest) = data.split_at(3 * LANE);
            let p = blk.as_ptr();
            let mut a = s as u64;
            let mut b = 0u64;
            let mut c = 0u64;
            let mut i = 0usize;
            while i < LANE {
                // SAFETY: `blk` is exactly `3 * LANE` bytes and `i + 8 <= LANE`,
                // so all three 8-byte lane reads stay in bounds. x86-64 is
                // little-endian, so the unaligned native-order read matches
                // `u64::from_le_bytes` (this path is x86-64 only).
                a = _mm_crc32_u64(a, (p.add(i) as *const u64).read_unaligned());
                b = _mm_crc32_u64(b, (p.add(LANE + i) as *const u64).read_unaligned());
                c = _mm_crc32_u64(c, (p.add(2 * LANE + i) as *const u64).read_unaligned());
                i += 8;
            }
            s = zshift(zt, zshift(zt, a as u32) ^ b as u32) ^ c as u32;
            data = rest;
        }
    }
    let mut chunks = data.chunks_exact(8);
    let mut s64 = s as u64;
    for ch in &mut chunks {
        s64 = _mm_crc32_u64(s64, u64::from_le_bytes(ch.try_into().unwrap()));
    }
    let mut s = s64 as u32;
    for &b in chunks.remainder() {
        s = _mm_crc32_u8(s, b);
    }
    s
}

fn update_scalar(state: u32, data: &[u8]) -> u32 {
    let t = tables();
    let mut s = state;
    let mut chunks = data.chunks_exact(8);
    for c in &mut chunks {
        let lo = u32::from_le_bytes([c[0], c[1], c[2], c[3]]) ^ s;
        let hi = u32::from_le_bytes([c[4], c[5], c[6], c[7]]);
        s = t[7][(lo & 0xFF) as usize]
            ^ t[6][((lo >> 8) & 0xFF) as usize]
            ^ t[5][((lo >> 16) & 0xFF) as usize]
            ^ t[4][(lo >> 24) as usize]
            ^ t[3][(hi & 0xFF) as usize]
            ^ t[2][((hi >> 8) & 0xFF) as usize]
            ^ t[1][((hi >> 16) & 0xFF) as usize]
            ^ t[0][(hi >> 24) as usize];
    }
    for &b in chunks.remainder() {
        s = t[0][((s ^ b as u32) & 0xFF) as usize] ^ (s >> 8);
    }
    s
}

#[derive(Clone)]
pub struct Crc32 {
    state: u32,
}

impl Default for Crc32 {
    fn default() -> Self {
        Self::new()
    }
}

impl Crc32 {
    pub fn new() -> Self {
        Crc32 { state: 0xFFFF_FFFF }
    }

    pub fn update(&mut self, data: &[u8]) {
        #[cfg(target_arch = "x86_64")]
        {
            if has_sse42() {
                // SAFETY: `update_hw` requires SSE4.2, checked on the line above.
                self.state = unsafe { update_hw(self.state, data) };
                return;
            }
        }
        self.state = update_scalar(self.state, data);
    }

    pub fn finalize(self) -> u32 {
        self.state ^ 0xFFFF_FFFF
    }
}

pub fn crc32(data: &[u8]) -> u32 {
    let mut c = Crc32::new();
    c.update(data);
    c.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vector() {
        assert_eq!(crc32(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn empty_is_zero() {
        assert_eq!(crc32(b""), 0);
    }

    #[test]
    fn streaming_matches_oneshot() {
        let data = b"the quick brown fox jumps over the lazy dog";
        for split in 0..data.len() {
            let mut c = Crc32::new();
            c.update(&data[..split]);
            c.update(&data[split..]);
            assert_eq!(c.finalize(), crc32(data), "split at {split}");
        }
    }

    #[test]
    fn scalar_matches_hardware() {
        let lens = [
            0usize,
            1,
            7,
            8,
            9,
            16,
            100,
            4096,
            3 * LANE - 1,
            3 * LANE,
            3 * LANE + 1,
            3 * LANE + 7,
            3 * LANE + 8,
            2 * 3 * LANE - 1,
            2 * 3 * LANE,
            2 * 3 * LANE + 13,
            10 * 3 * LANE + 5,
        ];
        for len in lens {
            let data: Vec<u8> = (0..len).map(|i| (i * 31 + 7) as u8).collect();
            let scalar = {
                let s = update_scalar(0xFFFF_FFFF, &data);
                s ^ 0xFFFF_FFFF
            };
            assert_eq!(scalar, crc32(&data), "len {len}");
        }
    }

    #[test]
    fn hardware_matches_scalar_unaligned_starts() {
        let data: Vec<u8> = (0..4 * 3 * LANE + 32).map(|i| (i * 131 + 3) as u8).collect();
        for off in 0..16 {
            let slice = &data[off..];
            let scalar = update_scalar(0xFFFF_FFFF, slice) ^ 0xFFFF_FFFF;
            assert_eq!(scalar, crc32(slice), "start offset {off}");
        }
    }

    #[test]
    fn streaming_splits_across_superblocks() {
        let data: Vec<u8> = (0..5 * 3 * LANE + 11).map(|i| (i * 7 + 1) as u8).collect();
        let expect = crc32(&data);
        for split in [
            1usize,
            8,
            LANE,
            3 * LANE - 1,
            3 * LANE,
            3 * LANE + 1,
            4 * LANE,
            2 * 3 * LANE,
            data.len() - 1,
        ] {
            let mut c = Crc32::new();
            c.update(&data[..split]);
            c.update(&data[split..]);
            assert_eq!(c.finalize(), expect, "split at {split}");
        }
    }

    #[test]
    fn zshift_is_lane_zero_bytes() {
        let zt = zshift_tables();
        let zeros = vec![0u8; LANE];
        for s in [0u32, 1, 0xFFFF_FFFF, 0xDEAD_BEEF, 0x8000_0000, 0x1234_5678] {
            assert_eq!(zshift(zt, s), update_scalar(s, &zeros), "state {s:#x}");
        }
    }

    #[test]
    #[ignore]
    fn bench_crc_3way_throughput() {
        for size in [4096usize, 64 * 1024, 1024 * 1024] {
            let data: Vec<u8> = (0..size).map(|i| (i * 31 + 7) as u8).collect();
            let iters = (512 * 1024 * 1024 / size).max(64);
            let mut sink = crc32(&data);
            let t0 = std::time::Instant::now();
            for _ in 0..iters {
                sink ^= crc32(&data);
            }
            let three_way = t0.elapsed();
            let t1 = std::time::Instant::now();
            for _ in 0..iters {
                let mut c = Crc32::new();
                for chunk in data.chunks(3 * LANE - 8) {
                    c.update(chunk);
                }
                sink ^= c.finalize();
            }
            let single = t1.elapsed();
            let gbps = |d: std::time::Duration| {
                (size as f64 * iters as f64) / d.as_secs_f64() / 1e9
            };
            println!(
                "crc32c {size:>8} B: single-stream {:.2} GB/s, 3-way {:.2} GB/s ({:.2}x) [sink {sink:08x}]",
                gbps(single),
                gbps(three_way),
                gbps(three_way) / gbps(single),
            );
        }
    }
}
