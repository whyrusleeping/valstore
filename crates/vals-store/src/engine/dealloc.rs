use super::*;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeallocStepStats {
    pub segments: usize,
    pub bytes: u64,
    pub errors: u64,
    pub unsupported: bool,
}

impl<D: BlockDevice> Engine<D> {
    pub fn set_device_dealloc(&mut self, enabled: bool) {
        self.alloc.set_dealloc_tracking(enabled);
    }

    pub fn dealloc_step(&mut self, max_segments: usize) -> DeallocStepStats {
        let mut stats = DeallocStepStats::default();
        if self.poisoned || !self.alloc.dealloc_tracking() || max_segments == 0 {
            return stats;
        }
        let segs = self.alloc.take_dealloc_candidates(max_segments);
        if segs.is_empty() { return stats; }
        let mut ranges: Vec<(u64, u64)> = Vec::new();
        for seg in &segs {
            let (lba, blocks) = self.alloc.segment_range(*seg);
            if let Some((start, len)) = ranges.last_mut() {
                if *start + *len == lba {
                    *len += blocks;
                    continue;
                }
            }
            ranges.push((lba, blocks));
        }
        match self.dev.deallocate(&ranges) {
            Ok(true) => {
                stats.segments = segs.len();
                stats.bytes = segs.len() as u64 * self.alloc.blocks_per_segment() * self.block_size as u64;
            }
            Ok(false) => {
                self.alloc.set_dealloc_tracking(false);
                stats.unsupported = true;
            }
            Err(e) => {
                stats.errors = 1;
                if !self.dealloc_error_warned {
                    log::warn!("engine: device deallocation failed: {e} (further errors suppressed)");
                    self.dealloc_error_warned = true;
                }
            }
        }
        stats
    }
}
