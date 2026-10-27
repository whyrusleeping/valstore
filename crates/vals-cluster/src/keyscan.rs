use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::time::Instant;

use vals_spdk::BlockDevice;
use vals_store::index::IndexEntry;
use vals_store::Engine;

use crate::drive::{self, DriveSet};
use crate::zonestore::{open_partition_zone_stores, RegionZoneConfig};

mod sha256;
#[cfg(test)]
mod tests;

type Error = Box<dyn std::error::Error>;
pub type CensusId = [u8; 12];

pub struct KeyscanConfig {
    pub node_id: u32,
    pub partitions: usize,
    pub region_blocks: u64,
    pub zones: RegionZoneConfig,
    pub zone_id: u32,
    pub partition: Option<usize>,
}

pub fn census_id(key: &[u8]) -> CensusId {
    sha256::sha256(key)[..12].try_into().unwrap()
}

pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes { write!(s, "{b:02x}").unwrap(); }
    s
}

fn quote(s: &str) -> String {
    use std::fmt::Write;
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if c < ' ' => { write!(out, "\\u{:04x}", c as u32).unwrap(); }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

pub fn read_want_ids(path: &Path) -> Result<HashSet<CensusId>, Error> {
    let mut out = HashSet::new();
    for (i, line) in BufReader::new(File::open(path)?).lines().enumerate() {
        let line = line?;
        let text = line.trim();
        if text.len() != 24 || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!("{}:{}: expected a 24-hex id", path.display(), i + 1).into());
        }
        let mut id = [0u8; 12];
        for (j, byte) in id.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&text[j*2..j*2+2], 16)?;
        }
        out.insert(id);
    }
    Ok(out)
}

fn exclusive(path: &Path) -> io::Result<BufWriter<File>> {
    Ok(BufWriter::new(OpenOptions::new().write(true).create_new(true).open(path)?))
}

fn entry_json(e: &IndexEntry) -> String {
    format!("{{\"version\":\"{:032x}\",\"seq\":{},\"tombstone\":{},\"ptr\":{{\"segment\":{},\"offset\":{},\"len\":{}}}}}",
        e.version, e.seq, e.tombstone, e.ptr.segment.0, e.ptr.offset, e.ptr.len)
}

fn details<D: BlockDevice>(engine: &Engine<D>, partition: usize, key: &[u8], winner: &IndexEntry) -> Result<String, Error> {
    let mut versions = Vec::new();
    for v in engine.key_versions(key)? {
        let source = match v.run { Some(i) => format!("{i}"), None => "null".into() };
        let handle = match v.handle {
            Some(h) => format!("{{\"len\":{},\"segments\":[{}]}}", h.len,
                h.segs.iter().map(|s| s.0.to_string()).collect::<Vec<_>>().join(",")),
            None => "null".into(),
        };
        let record = match v.record {
            Ok(r) => format!("{{\"status\":\"ok\",\"key_hex\":\"{}\",\"key_matches\":{},\"kind\":{},\"version\":\"{:032x}\",\"seq\":{},\"tombstone\":{},\"value_length\":{}}}",
                hex(&r.key), r.key == key, quote(&format!("{:?}", r.kind)), r.version, r.seq, r.tombstone, r.value.len()),
            Err(e) => format!("{{\"status\":{}}}", quote(&e.to_string())),
        };
        versions.push(format!("{{\"source\":{},\"run\":{},\"handle\":{},\"entry\":{},\"record\":{}}}",
            quote(if v.run.is_some() { "run" } else { "memtable" }), source, handle, entry_json(&v.entry), record));
    }
    Ok(format!("{{\"partition\":{},\"key_hex\":\"{}\",\"key_text\":{},\"id\":\"{}\",\"winner\":{},\"versions\":[{}]}}",
        partition, hex(key), quote(&String::from_utf8_lossy(key)), hex(&census_id(key)), entry_json(winner), versions.join(",")))
}

pub fn keyscan<DS: DriveSet>(ds: &DS, cfg: &KeyscanConfig, out: &Path, want: Option<&HashSet<CensusId>>) -> Result<(), Error> {
    if cfg.partitions == 0 || cfg.region_blocks == 0 || ds.drive_count() == 0 {
        return Err("keyscan requires nonzero partitions, region size and drives".into());
    }
    if cfg.partition.is_some_and(|p| p >= cfg.partitions) {
        return Err("partition out of range".into());
    }
    let layout = drive::plan(&drive::drive_caps(ds), cfg.region_blocks, cfg.partitions, cfg.partitions)?;
    let providers = open_partition_zone_stores(ds, &layout, &cfg.zones, cfg.node_id, false)?;
    std::fs::create_dir_all(out)?;
    let mut summary = exclusive(&out.join("summary.json"))?;
    let mut outputs = Vec::new();
    for p in layout.partitions().iter().filter(|p| cfg.partition.is_none_or(|n| n == p.partition)) {
        let (ids, tombs, matches) = if want.is_some() {
            (None, None, Some(exclusive(&out.join(format!("p{}.jsonl", p.partition)))?))
        } else {
            (Some(exclusive(&out.join(format!("p{}.ids", p.partition)))?),
             Some(exclusive(&out.join(format!("p{}.tomb.ids", p.partition)))?), None)
        };
        outputs.push((p, ids, tombs, matches));
    }
    let mut reports = Vec::new();
    let mut failed = false;
    for (p, mut ids, mut tombs, mut matches) in outputs {
        let start = Instant::now();
        let mut live = 0u64;
        let mut tomb = 0u64;
        let mut matched = 0u64;
        let mut runs = None;
        let mut generation = None;
        let result = (|| -> Result<(), Error> {
            let engine = providers[p.partition].open_engine_readonly(cfg.zone_id)?;
            runs = Some(engine.diagnostic_run_count());
            generation = Some(engine.manifest_generation());
            let mut lo: Option<Vec<u8>> = None;
            loop {
                let (page, more) = engine.scan_winners_page(lo.as_deref(), None, 4096, usize::MAX)?;
                for (key, entry) in &page {
                    let id = census_id(key);
                    if entry.tombstone {
                        tomb += 1;
                        if let Some(w) = &mut tombs { w.write_all(&id)?; }
                    } else {
                        live += 1;
                        if let Some(w) = &mut ids { w.write_all(&id)?; }
                    }
                    if want.is_some_and(|s| s.contains(&id)) {
                        writeln!(matches.as_mut().unwrap(), "{}", details(&engine, p.partition, key, entry)?)?;
                        matched += 1;
                    }
                }
                if !more { break; }
                let mut next = page.last().ok_or("empty scan page with more=true")?.0.clone();
                next.push(0);
                lo = Some(next);
            }
            Ok(())
        })();
        let mut error = result.err().map(|e| e.to_string());
        for w in [&mut ids, &mut tombs, &mut matches].into_iter().flatten() {
            if let Err(e) = w.flush() { error.get_or_insert(e.to_string()); }
        }
        failed |= error.is_some();
        reports.push(format!("{{\"partition\":{},\"drive\":{},\"base\":{},\"blocks\":{},\"live\":{},\"tombstones\":{},\"matched\":{},\"runs\":{},\"manifest_generation\":{},\"elapsed_ms\":{},\"error\":{}}}",
            p.partition, p.drive, p.base, p.len, live, tomb, matched,
            runs.map_or("null".into(), |n| n.to_string()), generation.map_or("null".into(), |n| n.to_string()),
            start.elapsed().as_millis(), error.map_or("null".into(), |s| quote(&s))));
    }
    writeln!(summary, "{{\"node_id\":{},\"zone_id\":{},\"id_format\":\"sha256-96 big-endian binary\",\"pass\":{},\"partitions\":[{}]}}",
        cfg.node_id, cfg.zone_id, if want.is_some() { 2 } else { 1 }, reports.join(","))?;
    summary.flush()?;
    if failed { return Err("one or more partitions failed; see summary.json (counts may be partial)".into()); }
    Ok(())
}
