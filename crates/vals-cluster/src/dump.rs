use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use vals_spdk::BlockDevice;
use vals_store::range_tombstone::{Fragment, FragmentSet};
use vals_store::{Config, Record, RecordKind};

const MAGIC_V1: &[u8; 6] = b"VDUMP\x01";
const MAGIC_V2: &[u8; 6] = b"VDUMP\x02";
const TOMB_SENTINEL: u32 = 0xFFFF_FFFF;
const UNBOUNDED: u32 = 0xFFFF_FFFF;
const RANGE_SENTINEL: u32 = 0xFFFF_FFFE;

#[derive(Debug, Clone, Copy, Default)]
pub struct DumpStats {
    pub segments: u64,
    pub puts: u64,
    pub tombstones: u64,
    pub range_tombstones: u64,
    pub covered: u64,
    pub value_bytes: u64,
    pub elapsed: Duration,
}

impl DumpStats {
    pub fn summary(
        &self,
        zone_name: &str,
        out: &Path,
        skip_tombstones: bool,
        workers: usize,
        rate_mib: Option<u64>,
    ) -> String {
        let total = self.puts + self.tombstones;
        let secs = self.elapsed.as_secs_f64();
        let workers = workers.max(1);
        let rate = rate_mib
            .map(|n| format!(", rate limit {n} MiB/s"))
            .unwrap_or_default();
        let ranges = if self.range_tombstones > 0 {
            format!(
                ", {} range tombstones, {} covered records skipped",
                self.range_tombstones, self.covered
            )
        } else {
            String::new()
        };
        format!(
            "zone {zone_name:?} -> {}: {} segments, {total} records \
             ({} put, {} tomb{}{ranges}), {:.2} GiB of values in {:.1}s ({:.0} rec/s, {} worker{}{rate})",
            out.display(),
            self.segments,
            self.puts,
            self.tombstones,
            if skip_tombstones {
                ", tombstones skipped"
            } else {
                ""
            },
            self.value_bytes as f64 / (1u64 << 30) as f64,
            secs,
            total as f64 / secs.max(1e-9),
            workers,
            if workers == 1 { "" } else { "s" },
        )
    }
}

pub fn emit_dump<D: BlockDevice>(
    dev: &D,
    scan_cfg: &Config,
    zone_name: &str,
    out: &Path,
    skip_tombstones: bool,
    workers: usize,
    rate_mib: Option<u64>,
) -> Result<DumpStats, String> {
    let pacer = rate_mib
        .map(|n| vals_store::ReadPacer::new(n).ok_or("rate_mib must be positive"))
        .transpose()?;
    let start = Instant::now();
    let rts = vals_store::read_range_tombstones(dev, scan_cfg, pacer.as_ref())
        .map_err(|e| format!("read range tombstones: {e}"))?;
    let file = File::create(out).map_err(|e| format!("create {}: {e}", out.display()))?;
    let mut w = BufWriter::with_capacity(1 << 20, file);
    write_header(&mut w, zone_name, !rts.is_empty()).map_err(|e| format!("write header: {e}"))?;
    for fragment in rts.fragments() {
        write_range(&mut w, &fragment).map_err(|e| format!("write range tombstone: {e}"))?;
    }

    let mut puts: u64 = 0;
    let mut tombstones: u64 = 0;
    let mut covered: u64 = 0;
    let mut value_bytes: u64 = 0;
    let mut next_progress = 1u64 << 20;

    let scan_stats = if workers <= 1 {
        let mut emit = |rec: &Record| -> std::io::Result<()> {
            encode_record(
                &mut w,
                rec,
                &rts,
                skip_tombstones,
                &mut puts,
                &mut tombstones,
                &mut covered,
                &mut value_bytes,
            )?;
            maybe_progress(
                start,
                zone_name,
                puts,
                tombstones,
                value_bytes,
                &mut next_progress,
            );
            Ok(())
        };
        match pacer.as_ref() {
            Some(pacer) => vals_store::scan_vlog_records_paced(dev, scan_cfg, pacer, &mut emit),
            None => vals_store::scan_vlog_records(dev, scan_cfg, &mut emit),
        }
        .map_err(|e| format!("vlog scan: {e}"))?
    } else {
        let map = |recs: &[(u32, Record)]| -> std::io::Result<Chunk> {
            let mut frag = Chunk::default();
            for (_off, rec) in recs {
                encode_record(
                    &mut frag.bytes,
                    rec,
                    &rts,
                    skip_tombstones,
                    &mut frag.puts,
                    &mut frag.tombstones,
                    &mut frag.covered,
                    &mut frag.value_bytes,
                )?;
            }
            Ok(frag)
        };
        let mut emit = |frag: Chunk| -> std::io::Result<()> {
            w.write_all(&frag.bytes)?;
            puts += frag.puts;
            tombstones += frag.tombstones;
            covered += frag.covered;
            value_bytes += frag.value_bytes;
            maybe_progress(
                start,
                zone_name,
                puts,
                tombstones,
                value_bytes,
                &mut next_progress,
            );
            Ok(())
        };
        match pacer.as_ref() {
            Some(pacer) => vals_store::scan_vlog_segments_parallel_paced(
                dev, scan_cfg, workers, pacer, map, &mut emit,
            ),
            None => vals_store::scan_vlog_segments_parallel(
                dev, scan_cfg, workers, map, &mut emit,
            ),
        }
        .map_err(|e| format!("vlog scan: {e}"))?
    };

    w.flush()
        .map_err(|e| format!("flush {}: {e}", out.display()))?;

    Ok(DumpStats {
        segments: scan_stats.segments,
        puts,
        tombstones,
        range_tombstones: rts.len() as u64,
        covered,
        value_bytes,
        elapsed: start.elapsed(),
    })
}

#[derive(Default)]
struct Chunk {
    bytes: Vec<u8>,
    puts: u64,
    tombstones: u64,
    covered: u64,
    value_bytes: u64,
}

#[allow(clippy::too_many_arguments)]
fn encode_record(
    w: &mut impl Write,
    rec: &Record,
    rts: &FragmentSet,
    skip_tombstones: bool,
    puts: &mut u64,
    tombstones: &mut u64,
    covered: &mut u64,
    value_bytes: &mut u64,
) -> std::io::Result<()> {
    if rec.kind == RecordKind::RangeDelete {
        return Ok(());
    }
    if !rts.is_empty() && rts.cover(&rec.key).is_some_and(|cover| rec.version <= cover) {
        *covered += 1;
        return Ok(());
    }
    let is_put = matches!(rec.kind, RecordKind::Put);
    let (blob_is_put, value) = if rec.value.is_empty() {
        (false, &rec.value[..])
    } else {
        crate::split_stored_blob(&rec.value)
    };
    let put = is_put && blob_is_put;
    if put {
        write_record(w, &rec.key, value)?;
        *puts += 1;
        *value_bytes += value.len() as u64;
    } else {
        *tombstones += 1;
        if !skip_tombstones {
            write_tombstone(w, &rec.key)?;
        }
    }
    Ok(())
}

fn maybe_progress(
    start: Instant,
    zone_name: &str,
    puts: u64,
    tombstones: u64,
    value_bytes: u64,
    next_progress: &mut u64,
) {
    let total = puts + tombstones;
    if total >= *next_progress {
        let secs = start.elapsed().as_secs_f64();
        eprint!(
            "\r  {zone_name}: {total} records ({puts} put, {tombstones} tomb), \
             {:.2} GiB, {:.0} rec/s   ",
            value_bytes as f64 / (1u64 << 30) as f64,
            total as f64 / secs.max(1e-9),
        );
        *next_progress += 1 << 20;
    }
}

fn write_header(w: &mut impl Write, zone_name: &str, ranges: bool) -> std::io::Result<()> {
    w.write_all(if ranges { MAGIC_V2 } else { MAGIC_V1 })?;
    let nb = zone_name.as_bytes();
    let nlen = nb.len().min(u8::MAX as usize);
    w.write_all(&[nlen as u8])?;
    w.write_all(&nb[..nlen])
}

fn write_record(w: &mut impl Write, k: &[u8], v: &[u8]) -> std::io::Result<()> {
    w.write_all(&(k.len() as u32).to_le_bytes())?;
    w.write_all(k)?;
    w.write_all(&(v.len() as u32).to_le_bytes())?;
    w.write_all(v)
}

fn write_tombstone(w: &mut impl Write, k: &[u8]) -> std::io::Result<()> {
    w.write_all(&(k.len() as u32).to_le_bytes())?;
    w.write_all(k)?;
    w.write_all(&TOMB_SENTINEL.to_le_bytes())
}

fn write_range(w: &mut impl Write, f: &Fragment) -> std::io::Result<()> {
    let bound = |w: &mut dyn Write, b: Option<&[u8]>| -> std::io::Result<()> {
        match b {
            Some(b) => {
                w.write_all(&(b.len() as u32).to_le_bytes())?;
                w.write_all(b)
            }
            None => w.write_all(&UNBOUNDED.to_le_bytes()),
        }
    };
    bound(w, (!f.lo.is_empty()).then_some(f.lo.as_slice()))?;
    w.write_all(&RANGE_SENTINEL.to_le_bytes())?;
    bound(w, f.hi.as_deref())?;
    w.write_all(&f.version.to_le_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use vals_spdk::MemBlockDevice;
    use vals_store::{Engine, Ordering, RunBuildOpts};

    enum Rec {
        Range(Option<Vec<u8>>, Option<Vec<u8>>, u128),
        Put(Vec<u8>, Vec<u8>),
        Tomb(Vec<u8>),
    }

    struct Cursor<'a> {
        b: &'a [u8],
        off: usize,
    }

    impl Cursor<'_> {
        fn take(&mut self, n: usize) -> Vec<u8> {
            let s = self.b[self.off..self.off + n].to_vec();
            self.off += n;
            s
        }

        fn u32(&mut self) -> u32 {
            u32::from_le_bytes(self.take(4).try_into().unwrap())
        }
    }

    fn parse(bytes: &[u8]) -> (Vec<u8>, Vec<Rec>) {
        let mut c = Cursor { b: bytes, off: 0 };
        let magic = c.take(6);
        let nlen = c.take(1)[0] as usize;
        c.take(nlen);
        let mut recs = Vec::new();
        while c.off < bytes.len() {
            let first = c.u32();
            let key = (first != UNBOUNDED).then(|| c.take(first as usize));
            let second = c.u32();
            if second == RANGE_SENTINEL {
                let hi_len = c.u32();
                let hi = (hi_len != UNBOUNDED).then(|| c.take(hi_len as usize));
                let version = u128::from_le_bytes(c.take(16).try_into().unwrap());
                recs.push(Rec::Range(key, hi, version));
                continue;
            }
            let key = key.expect("only a range record has an unbounded first field");
            if second == TOMB_SENTINEL {
                recs.push(Rec::Tomb(key));
            } else {
                recs.push(Rec::Put(key, c.take(second as usize)));
            }
        }
        (magic, recs)
    }

    fn replay(recs: &[Rec]) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let mut map = BTreeMap::new();
        for rec in recs {
            match rec {
                Rec::Range(lo, hi, _) => map.retain(|k: &Vec<u8>, _| {
                    lo.as_ref().is_some_and(|lo| k < lo) || hi.as_ref().is_some_and(|hi| k >= hi)
                }),
                Rec::Put(k, v) => {
                    map.insert(k.clone(), v.clone());
                }
                Rec::Tomb(k) => {
                    map.remove(k);
                }
            }
        }
        map
    }

    fn v(n: u64) -> u128 {
        (n as u128) << 64 | 7
    }

    fn blob(value: &[u8]) -> Vec<u8> {
        [&[vals_client::types::KIND_PUT][..], value].concat()
    }

    #[test]
    fn dump_emits_range_tombstones_first_and_skips_values_relocated_past_them() {
        if crate::testutil::isolated_env_test(&[
            ("VALS_VLOG_CO_COMPACT", "1"),
            ("VALS_MERGE_TIER_MIN_K", "2"),
            ("VALS_MERGE_FORCE_RUNS", ""),
        ]) {
            return;
        }
        let cfg = Config { seg_blocks: 8, reserved_blocks: 0, ordering: Ordering::Sorted };
        let dev = MemBlockDevice::new(4096, 1 << 14);
        let mut e = Engine::create_prewiped(dev.share(), cfg);
        let key = |i: u32| format!("a{i:04}").into_bytes();
        for i in 0..40u32 {
            e.put(&key(i), &blob(&[b'a'; 300]), v(1)).unwrap();
        }
        e.flush(RunBuildOpts::default()).unwrap();
        e.apply_range_delete(Some(&key(5)), Some(&key(25)), v(5)).unwrap();
        e.put(&key(10), &blob(b"newer"), v(9)).unwrap();
        e.put(&key(30), &blob(b"second"), v(2)).unwrap();
        e.delete(&key(31), v(3)).unwrap();
        for i in 0..40u32 {
            e.put(format!("b{i:04}").as_bytes(), &blob(&[b'b'; 300]), v(2)).unwrap();
        }
        e.flush(RunBuildOpts::default()).unwrap();
        let plan = e.compact_plan(RunBuildOpts::default(), None).unwrap().expect("a merge of both runs");
        assert!(plan.co_compacts(), "test premise: the merge relocates its inputs' values");
        let written = vals_rt::block_on(
            vals_store::compact_and_write(plan, Arc::new(e.device().sibling().unwrap())),
            || {},
        )
        .unwrap();
        e.compact_commit(written).unwrap();
        e.sync().unwrap();

        let mut raw: Vec<(RecordKind, Vec<u8>, u128)> = Vec::new();
        vals_store::scan_vlog_records(&dev, &cfg, |rec: &Record| {
            raw.push((rec.kind, rec.key.clone(), rec.version));
            Ok::<(), std::convert::Infallible>(())
        })
        .unwrap();
        let last_rt = raw.iter().rposition(|(kind, _, _)| *kind == RecordKind::RangeDelete);
        let last_covered = raw.iter().rposition(|(kind, k, version)| {
            *kind == RecordKind::Put && *k == key(7) && *version == v(1)
        });
        assert!(
            last_covered.is_some_and(|at| last_rt.is_none_or(|rt| at > rt)),
            "test premise: a covered value sits past its range tombstone in the log ({last_covered:?}, {last_rt:?})"
        );
        assert_eq!(e.get(&key(7)).unwrap(), None);

        let dir = std::env::temp_dir().join(format!("vals-dump-rt-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let serial = dir.join("serial.vdump");
        let parallel = dir.join("parallel.vdump");
        let stats = emit_dump(&dev, &cfg, "rt", &serial, false, 1, None).unwrap();
        emit_dump(&dev, &cfg, "rt", &parallel, false, 4, None).unwrap();
        let bytes = std::fs::read(&serial).unwrap();
        assert_eq!(bytes, std::fs::read(&parallel).unwrap(), "parallel dump is byte-identical");
        let _ = std::fs::remove_dir_all(&dir);

        let (magic, recs) = parse(&bytes);
        assert_eq!(magic, MAGIC_V2);
        assert!(matches!(&recs[0], Rec::Range(Some(lo), Some(hi), ver) if *lo == key(5) && *hi == key(25) && *ver == v(5)));
        assert!(recs[1..].iter().all(|r| !matches!(r, Rec::Range(..))), "range records come first");

        let mut want = BTreeMap::new();
        for k in (0..40u32).map(key).chain((0..40u32).map(|i| format!("b{i:04}").into_bytes())) {
            if let Some(stored) = e.get(&k).unwrap() {
                want.insert(k, crate::split_stored_blob(&stored).1.to_vec());
            }
        }
        assert_eq!(want.get(&key(10)).map(Vec::as_slice), Some(&b"newer"[..]));
        let got = replay(&recs);
        let differ: Vec<String> = got
            .keys()
            .chain(want.keys())
            .filter(|k| got.get(*k) != want.get(*k))
            .map(|k| String::from_utf8_lossy(k).into_owned())
            .collect();
        assert!(differ.is_empty(), "replaying the dump must reproduce the store; keys that differ: {differ:?}");
        assert_eq!((stats.range_tombstones, stats.covered > 0), (1, true), "{stats:?}");
    }

    #[test]
    fn a_dump_without_range_tombstones_keeps_the_v1_layout() {
        let cfg = Config { seg_blocks: 8, reserved_blocks: 0, ordering: Ordering::Sorted };
        let dev = MemBlockDevice::new(4096, 1 << 12);
        let mut e = Engine::create_prewiped(dev.share(), cfg);
        e.put(b"k", &blob(b"v"), v(1)).unwrap();
        e.put(b"gone", &blob(b"g"), v(1)).unwrap();
        e.delete(b"gone", v(2)).unwrap();
        e.sync().unwrap();
        let out = std::env::temp_dir().join(format!("vals-dump-v1-{}.vdump", std::process::id()));
        let stats = emit_dump(&dev, &cfg, "plain", &out, false, 1, None).unwrap();
        let bytes = std::fs::read(&out).unwrap();
        let _ = std::fs::remove_file(&out);
        assert_eq!((stats.puts, stats.tombstones, stats.range_tombstones), (2, 1, 0));
        let mut want = MAGIC_V1.to_vec();
        want.push(5);
        want.extend_from_slice(b"plain");
        for (k, v) in [(&b"k"[..], Some(&b"v"[..])), (b"gone", Some(b"g")), (b"gone", None)] {
            want.extend_from_slice(&(k.len() as u32).to_le_bytes());
            want.extend_from_slice(k);
            match v {
                Some(v) => {
                    want.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    want.extend_from_slice(v);
                }
                None => want.extend_from_slice(&TOMB_SENTINEL.to_le_bytes()),
            }
        }
        assert_eq!(bytes, want);
    }
}
