use crate::alloc::SegmentId;
use crate::crc;
use crate::run::RunHandle;

const MAN_MAGIC: u32 = 0x4e41_4d56;
const MAN_VERSION_V1: u16 = 1;
const MAN_VERSION_V2: u16 = 2;
const MAN_VERSION_V3: u16 = 3;
const MAN_VERSION: u16 = 4;
pub(crate) const UNSUPPORTED_VERSION: &str = "unsupported version";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunMeta {
    pub handle: RunHandle,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub entry_count: u64,
    pub level: u32,
    pub value_segs: Option<Vec<SegmentId>>,
    pub value_bytes: Option<u64>,
    pub tombstone_min_version: Option<u128>,
    pub value_seg_bytes: Option<Vec<u64>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub generation: u64,
    pub covered_seq: u64,
    pub covered_epoch: u64,
    pub runs: Vec<RunMeta>,
}

impl Manifest {
    pub fn empty() -> Self {
        Manifest {
            generation: 0,
            covered_seq: 0,
            covered_epoch: 0,
            runs: Vec::new(),
        }
    }
}

pub fn encode(m: &Manifest) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&MAN_MAGIC.to_le_bytes());
    b.extend_from_slice(&MAN_VERSION.to_le_bytes());
    b.extend_from_slice(&m.generation.to_le_bytes());
    b.extend_from_slice(&m.covered_seq.to_le_bytes());
    b.extend_from_slice(&m.covered_epoch.to_le_bytes());
    b.extend_from_slice(&(m.runs.len() as u32).to_le_bytes());
    for r in &m.runs {
        b.extend_from_slice(&r.level.to_le_bytes());
        b.extend_from_slice(&r.entry_count.to_le_bytes());
        b.extend_from_slice(&(r.min_key.len() as u32).to_le_bytes());
        b.extend_from_slice(&r.min_key);
        b.extend_from_slice(&(r.max_key.len() as u32).to_le_bytes());
        b.extend_from_slice(&r.max_key);
        b.extend_from_slice(&r.handle.len.to_le_bytes());
        b.extend_from_slice(&(r.handle.segs.len() as u32).to_le_bytes());
        for s in &r.handle.segs {
            b.extend_from_slice(&s.0.to_le_bytes());
        }
        match r.value_bytes {
            Some(vb) => {
                b.push(1);
                b.extend_from_slice(&vb.to_le_bytes());
            }
            None => b.push(0),
        }
        match &r.value_segs {
            Some(vs) => {
                b.push(1);
                b.extend_from_slice(&(vs.len() as u32).to_le_bytes());
                for s in vs {
                    b.extend_from_slice(&s.0.to_le_bytes());
                }
            }
            None => b.push(0),
        }
        match r.tombstone_min_version {
            Some(tmv) => {
                b.push(1);
                b.extend_from_slice(&tmv.to_le_bytes());
            }
            None => b.push(0),
        }
        match &r.value_seg_bytes {
            Some(vsb) => {
                b.push(1);
                b.extend_from_slice(&(vsb.len() as u32).to_le_bytes());
                for n in vsb {
                    b.extend_from_slice(&n.to_le_bytes());
                }
            }
            None => b.push(0),
        }
    }
    let crc = crc::crc32(&b);
    b.extend_from_slice(&crc.to_le_bytes());
    b
}

struct Cursor<'a> {
    b: &'a [u8],
    off: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], &'static str> {
        let end = self.off.checked_add(n).ok_or("length overflow")?;
        let s = self.b.get(self.off..end).ok_or("truncated")?;
        self.off = end;
        Ok(s)
    }
    fn u32(&mut self) -> Result<u32, &'static str> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u16(&mut self) -> Result<u16, &'static str> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u8(&mut self) -> Result<u8, &'static str> {
        Ok(self.take(1)?[0])
    }
    fn u64(&mut self) -> Result<u64, &'static str> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn u128(&mut self) -> Result<u128, &'static str> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
}

pub fn decode(bytes: &[u8]) -> Result<Manifest, &'static str> {
    if bytes.len() < 4 {
        return Err("truncated");
    }
    let body = &bytes[..bytes.len() - 4];
    let stored = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
    if crc::crc32(body) != stored {
        return Err("crc mismatch");
    }
    let mut c = Cursor { b: body, off: 0 };
    if c.u32()? != MAN_MAGIC {
        return Err("bad magic");
    }
    let version = c.u16()?;
    if version != MAN_VERSION_V1
        && version != MAN_VERSION_V2
        && version != MAN_VERSION_V3
        && version != MAN_VERSION
    {
        return Err(UNSUPPORTED_VERSION);
    }
    let generation = c.u64()?;
    let covered_seq = c.u64()?;
    let covered_epoch = c.u64()?;
    let run_count = c.u32()? as usize;
    let mut runs = Vec::with_capacity(run_count.min(1 << 16));
    for _ in 0..run_count {
        let level = c.u32()?;
        let entry_count = c.u64()?;
        let min_len = c.u32()? as usize;
        let min_key = c.take(min_len)?.to_vec();
        let max_len = c.u32()? as usize;
        let max_key = c.take(max_len)?.to_vec();
        let run_len = c.u64()?;
        let seg_count = c.u32()? as usize;
        let mut segs = Vec::with_capacity(seg_count.min(1 << 20));
        for _ in 0..seg_count {
            segs.push(SegmentId(c.u64()?));
        }
        let (value_bytes, value_segs) = if version >= MAN_VERSION_V2 {
            let value_bytes = if c.u8()? != 0 { Some(c.u64()?) } else { None };
            let value_segs = if c.u8()? != 0 {
                let n = c.u32()? as usize;
                let mut vs = Vec::with_capacity(n.min(1 << 20));
                for _ in 0..n {
                    vs.push(SegmentId(c.u64()?));
                }
                Some(vs)
            } else {
                None
            };
            (value_bytes, value_segs)
        } else {
            (None, None)
        };
        let tombstone_min_version = if version >= MAN_VERSION_V3 {
            if c.u8()? != 0 {
                Some(c.u128()?)
            } else {
                None
            }
        } else {
            None
        };
        let value_seg_bytes = if version >= MAN_VERSION {
            if c.u8()? != 0 {
                let n = c.u32()? as usize;
                let mut vsb = Vec::with_capacity(n.min(1 << 20));
                for _ in 0..n {
                    vsb.push(c.u64()?);
                }
                Some(vsb)
            } else {
                None
            }
        } else {
            None
        };
        runs.push(RunMeta {
            handle: RunHandle { segs, len: run_len },
            min_key,
            max_key,
            entry_count,
            level,
            value_segs,
            value_bytes,
            tombstone_min_version,
            value_seg_bytes,
        });
    }
    if c.off != body.len() {
        return Err("trailing bytes");
    }
    Ok(Manifest {
        generation,
        covered_seq,
        covered_epoch,
        runs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_meta(level: u32, segs: &[u64], min: &[u8], max: &[u8], n: u64) -> RunMeta {
        RunMeta {
            handle: RunHandle {
                segs: segs.iter().map(|s| SegmentId(*s)).collect(),
                len: 1234 + n,
            },
            min_key: min.to_vec(),
            max_key: max.to_vec(),
            entry_count: n,
            level,
            value_segs: Some(segs.iter().map(|s| SegmentId(s + 100)).collect()),
            value_bytes: Some(n * 7),
            tombstone_min_version: Some(u128::from(n) + 1),
            value_seg_bytes: Some(segs.iter().map(|_| n).collect()),
        }
    }

    fn sample() -> Manifest {
        Manifest {
            generation: 9,
            covered_seq: 4242,
            covered_epoch: 17,
            runs: vec![
                run_meta(0, &[3, 7], b"app.a", b"app.m", 5000),
                run_meta(1, &[10, 11, 12], b"app.n", b"app.z", 90000),
            ],
        }
    }

    #[test]
    fn round_trip_preserves_everything() {
        let m = sample();
        assert_eq!(decode(&encode(&m)).unwrap(), m);
    }

    #[test]
    fn round_trip_none_value_fields() {
        let m = Manifest {
            generation: 3,
            covered_seq: 1,
            covered_epoch: 1,
            runs: vec![RunMeta {
                handle: RunHandle {
                    segs: vec![SegmentId(1)],
                    len: 99,
                },
                min_key: b"a".to_vec(),
                max_key: b"z".to_vec(),
                entry_count: 7,
                level: 0,
                value_segs: None,
                value_bytes: None,
                tombstone_min_version: None,
                value_seg_bytes: None,
            }],
        };
        let d = decode(&encode(&m)).unwrap();
        assert_eq!(d, m);
        assert!(d.runs[0].value_segs.is_none());
        assert!(d.runs[0].value_bytes.is_none());
    }

    #[test]
    fn decodes_legacy_v1_manifest_as_none() {
        let mut b = Vec::new();
        b.extend_from_slice(&MAN_MAGIC.to_le_bytes());
        b.extend_from_slice(&MAN_VERSION_V1.to_le_bytes());
        b.extend_from_slice(&5u64.to_le_bytes());
        b.extend_from_slice(&42u64.to_le_bytes());
        b.extend_from_slice(&3u64.to_le_bytes());
        b.extend_from_slice(&1u32.to_le_bytes());
        b.extend_from_slice(&0u32.to_le_bytes());
        b.extend_from_slice(&9u64.to_le_bytes());
        b.extend_from_slice(&(3u32).to_le_bytes());
        b.extend_from_slice(b"app");
        b.extend_from_slice(&(3u32).to_le_bytes());
        b.extend_from_slice(b"zzz");
        b.extend_from_slice(&123u64.to_le_bytes());
        b.extend_from_slice(&2u32.to_le_bytes());
        b.extend_from_slice(&7u64.to_le_bytes());
        b.extend_from_slice(&8u64.to_le_bytes());
        let crc = crc::crc32(&b);
        b.extend_from_slice(&crc.to_le_bytes());

        let m = decode(&b).unwrap();
        assert_eq!(m.generation, 5);
        assert_eq!(m.runs.len(), 1);
        assert_eq!(m.runs[0].handle.segs, vec![SegmentId(7), SegmentId(8)]);
        assert!(m.runs[0].value_segs.is_none());
        assert!(m.runs[0].value_bytes.is_none());
    }

    #[test]
    fn decodes_legacy_v2_manifest_tombstone_min_none() {
        let mut b = Vec::new();
        b.extend_from_slice(&MAN_MAGIC.to_le_bytes());
        b.extend_from_slice(&MAN_VERSION_V2.to_le_bytes());
        b.extend_from_slice(&5u64.to_le_bytes());
        b.extend_from_slice(&42u64.to_le_bytes());
        b.extend_from_slice(&3u64.to_le_bytes());
        b.extend_from_slice(&1u32.to_le_bytes());
        b.extend_from_slice(&0u32.to_le_bytes());
        b.extend_from_slice(&9u64.to_le_bytes());
        b.extend_from_slice(&(3u32).to_le_bytes());
        b.extend_from_slice(b"app");
        b.extend_from_slice(&(3u32).to_le_bytes());
        b.extend_from_slice(b"zzz");
        b.extend_from_slice(&123u64.to_le_bytes());
        b.extend_from_slice(&2u32.to_le_bytes());
        b.extend_from_slice(&7u64.to_le_bytes());
        b.extend_from_slice(&8u64.to_le_bytes());
        b.push(1);
        b.extend_from_slice(&555u64.to_le_bytes());
        b.push(1);
        b.extend_from_slice(&2u32.to_le_bytes());
        b.extend_from_slice(&107u64.to_le_bytes());
        b.extend_from_slice(&108u64.to_le_bytes());
        let crc = crc::crc32(&b);
        b.extend_from_slice(&crc.to_le_bytes());

        let m = decode(&b).unwrap();
        assert_eq!(m.runs.len(), 1);
        assert_eq!(m.runs[0].value_bytes, Some(555));
        assert_eq!(
            m.runs[0].value_segs,
            Some(vec![SegmentId(107), SegmentId(108)])
        );
        assert!(m.runs[0].tombstone_min_version.is_none());
    }

    #[test]
    fn round_trip_v3_tombstone_min_version() {
        let m = sample();
        let d = decode(&encode(&m)).unwrap();
        assert_eq!(d, m);
        assert!(d.runs[0].tombstone_min_version.is_some());
        assert_eq!(
            d.runs[0].tombstone_min_version,
            m.runs[0].tombstone_min_version
        );
    }

    #[test]
    fn decodes_legacy_v3_manifest_value_seg_bytes_none() {
        let mut b = Vec::new();
        b.extend_from_slice(&MAN_MAGIC.to_le_bytes());
        b.extend_from_slice(&MAN_VERSION_V3.to_le_bytes());
        b.extend_from_slice(&5u64.to_le_bytes());
        b.extend_from_slice(&42u64.to_le_bytes());
        b.extend_from_slice(&3u64.to_le_bytes());
        b.extend_from_slice(&1u32.to_le_bytes());
        b.extend_from_slice(&0u32.to_le_bytes());
        b.extend_from_slice(&9u64.to_le_bytes());
        b.extend_from_slice(&(3u32).to_le_bytes());
        b.extend_from_slice(b"app");
        b.extend_from_slice(&(3u32).to_le_bytes());
        b.extend_from_slice(b"zzz");
        b.extend_from_slice(&123u64.to_le_bytes());
        b.extend_from_slice(&2u32.to_le_bytes());
        b.extend_from_slice(&7u64.to_le_bytes());
        b.extend_from_slice(&8u64.to_le_bytes());
        b.push(1);
        b.extend_from_slice(&555u64.to_le_bytes());
        b.push(1);
        b.extend_from_slice(&2u32.to_le_bytes());
        b.extend_from_slice(&107u64.to_le_bytes());
        b.extend_from_slice(&108u64.to_le_bytes());
        b.push(1);
        b.extend_from_slice(&77u128.to_le_bytes());
        let crc = crc::crc32(&b);
        b.extend_from_slice(&crc.to_le_bytes());

        let m = decode(&b).unwrap();
        assert_eq!(m.runs.len(), 1);
        assert_eq!(m.runs[0].value_bytes, Some(555));
        assert_eq!(m.runs[0].tombstone_min_version, Some(77));
        assert!(m.runs[0].value_seg_bytes.is_none());
    }

    #[test]
    fn round_trip_v4_value_seg_bytes() {
        let m = sample();
        let d = decode(&encode(&m)).unwrap();
        assert_eq!(d, m);
        assert!(d.runs[0].value_seg_bytes.is_some());
        assert_eq!(d.runs[0].value_seg_bytes, m.runs[0].value_seg_bytes);
    }

    #[test]
    fn round_trip_empty() {
        let m = Manifest::empty();
        assert_eq!(decode(&encode(&m)).unwrap(), m);
    }

    #[test]
    fn round_trip_empty_keys_and_no_segs() {
        let m = Manifest {
            generation: 1,
            covered_seq: 0,
            covered_epoch: 0,
            runs: vec![run_meta(0, &[], b"", b"", 0)],
        };
        assert_eq!(decode(&encode(&m)).unwrap(), m);
    }

    #[test]
    fn crc_mismatch_detected() {
        let mut bytes = encode(&sample());
        bytes[16] ^= 0xff;
        assert_eq!(decode(&bytes), Err("crc mismatch"));
    }

    #[test]
    fn truncation_is_err_not_panic() {
        let bytes = encode(&sample());
        for cut in [0usize, 1, 8, 20, bytes.len() - 1] {
            assert!(decode(&bytes[..cut]).is_err(), "cut {cut}");
        }
    }

    #[test]
    fn bad_magic_detected() {
        let mut bytes = encode(&sample());
        bytes[0] ^= 0xff;
        let body_len = bytes.len() - 4;
        let crc = crc::crc32(&bytes[..body_len]);
        bytes[body_len..].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(decode(&bytes), Err("bad magic"));
    }
}
