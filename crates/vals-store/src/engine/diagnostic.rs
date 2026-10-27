use super::*;

pub struct KeyVersion {
    pub run: Option<usize>,
    pub handle: Option<run::RunHandle>,
    pub entry: IndexEntry,
    pub record: Result<Record, LogError>,
}

impl<D: BlockDevice> Engine<D> {
    pub fn key_versions(&self, key: &[u8]) -> Result<Vec<KeyVersion>, EngineError> {
        let mut entries = Vec::new();
        if let Some(e) = self.index.get(key) {
            entries.push((None, None, e));
        }
        let mut hi = key.to_vec();
        hi.push(0);
        for (i, or) in self.runs.iter().enumerate() {
            let mut scan = or.run.block_scan(Some(key), Some(&hi));
            while let Some(e) = scan.next(&self.alloc)? {
                entries.push((Some(i), Some(or.meta.handle.clone()), IndexEntry {
                    ptr: e.ptr, seq: e.seq, version: e.version, tombstone: e.tombstone,
                }));
            }
        }
        Ok(entries.into_iter().map(|(run, handle, entry)| {
            let record = (|| -> Result<Record, LogError> {
                let ptr = entry.ptr;
                let bs = self.dev.block_size();
                let seg_bytes = self.alloc.blocks_per_segment() * bs as u64;
                if ptr.segment.0 >= self.alloc.num_segments()
                    || ptr.len == 0 || ptr.offset as u64 + ptr.len as u64 > seg_bytes
                {
                    return Err(LogError::Corrupt("ValuePtr outside segment"));
                }
                let first = ptr.offset as usize / bs;
                let within = ptr.offset as usize % bs;
                let mut buf = vec![0u8; (within + ptr.len as usize).div_ceil(bs) * bs];
                self.dev.read_at(self.alloc.segment_range(ptr.segment).0 + first as u64, &mut buf)?;
                crate::vlog::decode_record(&buf[within..within + ptr.len as usize])
            })();
            KeyVersion { run, handle, entry, record }
        }).collect())
    }

    pub fn diagnostic_run_count(&self) -> usize {
        self.runs.len()
    }
}
