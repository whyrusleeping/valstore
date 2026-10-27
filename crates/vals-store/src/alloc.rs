#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentId(pub u64);

#[derive(Debug, PartialEq, Eq)]
pub enum AllocError {
    OutOfRange {
        seg: u64,
        num_segments: u64,
    },
    AlreadyFree(u64),
    AlreadyUsed(u64),
}

impl std::fmt::Display for AllocError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AllocError::OutOfRange { seg, num_segments } => {
                write!(
                    f,
                    "segment {seg} out of range (num_segments={num_segments})"
                )
            }
            AllocError::AlreadyFree(s) => write!(f, "segment {s} is already free"),
            AllocError::AlreadyUsed(s) => write!(f, "segment {s} is already allocated"),
        }
    }
}

impl std::error::Error for AllocError {}

struct DeallocTracking {
    pending: std::collections::BTreeMap<SegmentId, u64>,
    free_stamp: u64,
    durable_through: u64,
}

pub struct Allocator {
    blocks_per_segment: u64,
    reserved_blocks: u64,
    num_segments: u64,
    free: Vec<u64>,
    free_count: u64,
    cursor_word: usize,
    data_reserve: u64,
    dealloc: Option<DeallocTracking>,
}

impl Allocator {
    pub fn new(device_blocks: u64, blocks_per_segment: u64, reserved_blocks: u64) -> Self {
        assert!(blocks_per_segment > 0, "blocks_per_segment must be > 0");
        let usable = device_blocks.saturating_sub(reserved_blocks);
        let num_segments = usable / blocks_per_segment;
        assert!(
            num_segments <= u32::MAX as u64 + 1,
            "num_segments ({num_segments}) exceeds the u32 packed-index-entry segment space"
        );

        let words = num_segments.div_ceil(64) as usize;
        let mut free = vec![u64::MAX; words];
        let rem = num_segments % 64;
        if rem != 0 {
            free[words - 1] = (1u64 << rem) - 1;
        }

        Allocator {
            blocks_per_segment,
            reserved_blocks,
            num_segments,
            free,
            free_count: num_segments,
            cursor_word: 0,
            data_reserve: 0,
            dealloc: None,
        }
    }

    pub fn set_dealloc_tracking(&mut self, enabled: bool) {
        if !enabled {
            self.dealloc = None;
        } else if self.dealloc.is_none() {
            self.dealloc = Some(DeallocTracking {
                pending: std::collections::BTreeMap::new(),
                free_stamp: 1,
                durable_through: 0,
            });
        }
    }

    pub fn dealloc_tracking(&self) -> bool {
        self.dealloc.is_some()
    }

    pub fn dealloc_barrier_begin(&mut self) -> u64 {
        let Some(state) = self.dealloc.as_mut() else { return 0; };
        let stamp = state.free_stamp;
        state.free_stamp += 1;
        stamp
    }

    pub fn dealloc_barrier_commit(&mut self, token: u64) {
        if let Some(state) = self.dealloc.as_mut() {
            state.durable_through = state.durable_through.max(token);
        }
    }

    pub(crate) fn take_dealloc_candidates(&mut self, max_segments: usize) -> Vec<SegmentId> {
        let Some(state) = self.dealloc.as_ref() else { return Vec::new(); };
        let segs: Vec<_> = state.pending.iter()
            .filter(|(seg, stamp)| **stamp <= state.durable_through && self.is_free(**seg))
            .take(max_segments).map(|(&seg, _)| seg).collect();
        let state = self.dealloc.as_mut().unwrap();
        for seg in &segs { state.pending.remove(seg); }
        segs
    }

    pub fn num_segments(&self) -> u64 {
        self.num_segments
    }

    pub fn free_segments(&self) -> u64 {
        self.free_count
    }

    pub fn used_segments(&self) -> u64 {
        self.num_segments - self.free_count
    }

    pub fn maint_reserve(&self) -> u64 {
        self.data_reserve
    }

    pub fn set_data_reserve(&mut self, n: u64) {
        self.data_reserve = n.min(self.num_segments);
    }

    pub fn blocks_per_segment(&self) -> u64 {
        self.blocks_per_segment
    }

    pub fn reserved_blocks(&self) -> u64 {
        self.reserved_blocks
    }

    pub fn device_blocks(&self) -> u64 {
        self.reserved_blocks + self.num_segments * self.blocks_per_segment
    }

    pub fn segment_range(&self, seg: SegmentId) -> (u64, u64) {
        assert!(seg.0 < self.num_segments, "segment {} out of range", seg.0);
        let start = self.reserved_blocks + seg.0 * self.blocks_per_segment;
        (start, self.blocks_per_segment)
    }

    pub fn is_free(&self, seg: SegmentId) -> bool {
        if seg.0 >= self.num_segments {
            return false;
        }
        let (w, b) = (seg.0 / 64, seg.0 % 64);
        (self.free[w as usize] >> b) & 1 == 1
    }

    pub fn used_list(&self) -> Vec<SegmentId> {
        let mut v = Vec::with_capacity(self.used_segments() as usize);
        for s in 0..self.num_segments {
            let seg = SegmentId(s);
            if !self.is_free(seg) {
                v.push(seg);
            }
        }
        v
    }

    pub fn allocate(&mut self) -> Option<SegmentId> {
        if self.free_count == 0 {
            return None;
        }
        self.allocate_unchecked()
    }

    pub fn allocate_data(&mut self) -> Option<SegmentId> {
        if self.free_count <= self.data_reserve {
            return None;
        }
        self.allocate_unchecked()
    }

    fn allocate_unchecked(&mut self) -> Option<SegmentId> {
        let words = self.free.len();
        for off in 0..words {
            let w = (self.cursor_word + off) % words;
            let bits = self.free[w];
            if bits != 0 {
                let bit = bits.trailing_zeros() as u64;
                let seg = w as u64 * 64 + bit;
                debug_assert!(seg < self.num_segments, "padding bit was set free");
                self.free[w] &= !(1u64 << bit);
                self.free_count -= 1;
                self.cursor_word = w;
                if let Some(state) = self.dealloc.as_mut() {
                    state.pending.remove(&SegmentId(seg));
                }
                return Some(SegmentId(seg));
            }
        }
        unreachable!("free_count={} but no free bit found", self.free_count)
    }

    pub fn free(&mut self, seg: SegmentId) -> Result<(), AllocError> {
        self.check_range(seg)?;
        let (w, b) = (seg.0 / 64, seg.0 % 64);
        if (self.free[w as usize] >> b) & 1 == 1 {
            return Err(AllocError::AlreadyFree(seg.0));
        }
        self.free[w as usize] |= 1u64 << b;
        self.free_count += 1;
        if let Some(state) = self.dealloc.as_mut() {
            state.pending.insert(seg, state.free_stamp);
        }
        Ok(())
    }

    pub fn mark_allocated(&mut self, seg: SegmentId) -> Result<(), AllocError> {
        self.check_range(seg)?;
        let (w, b) = (seg.0 / 64, seg.0 % 64);
        if (self.free[w as usize] >> b) & 1 == 0 {
            return Err(AllocError::AlreadyUsed(seg.0));
        }
        self.free[w as usize] &= !(1u64 << b);
        self.free_count -= 1;
        if let Some(state) = self.dealloc.as_mut() {
            state.pending.remove(&seg);
        }
        Ok(())
    }

    fn check_range(&self, seg: SegmentId) -> Result<(), AllocError> {
        if seg.0 >= self.num_segments {
            Err(AllocError::OutOfRange {
                seg: seg.0,
                num_segments: self.num_segments,
            })
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn dealloc_barrier_excludes_later_frees_and_reuse() {
        let mut a = Allocator::new(16, 1, 0);
        a.set_dealloc_tracking(true);
        let first = a.allocate().unwrap();
        let later = a.allocate().unwrap();
        a.free(first).unwrap();
        assert!(a.take_dealloc_candidates(16).is_empty());
        let token = a.dealloc_barrier_begin();
        a.free(later).unwrap();
        a.dealloc_barrier_commit(token);
        assert_eq!(a.take_dealloc_candidates(16), [first]);
        let token = a.dealloc_barrier_begin();
        a.dealloc_barrier_commit(token);
        a.mark_allocated(later).unwrap();
        assert!(a.take_dealloc_candidates(16).is_empty());
        a.free(later).unwrap();
        assert_eq!(a.allocate_data(), Some(first));
        assert_eq!(a.allocate(), Some(later));
        let token = a.dealloc_barrier_begin();
        a.dealloc_barrier_commit(token);
        assert!(a.take_dealloc_candidates(16).is_empty());
        a.free(later).unwrap();
        a.set_dealloc_tracking(false);
        a.set_dealloc_tracking(true);
        let token = a.dealloc_barrier_begin();
        a.dealloc_barrier_commit(token);
        assert!(a.take_dealloc_candidates(16).is_empty());
    }

    #[test]
    fn geometry_and_reserved() {
        let a = Allocator::new(1000, 64, 100);
        assert_eq!(a.num_segments(), 14);
        assert_eq!(a.free_segments(), 14);
        assert_eq!(a.used_segments(), 0);
        assert_eq!(a.segment_range(SegmentId(0)), (100, 64));
        assert_eq!(a.segment_range(SegmentId(1)), (164, 64));
        let (start, blocks) = a.segment_range(SegmentId(13));
        assert!(start + blocks <= 1000, "last segment must fit on device");
    }

    #[test]
    #[should_panic(expected = "exceeds the u32 packed-index-entry segment space")]
    fn too_many_segments_for_packed_entry_panics() {
        let _ = Allocator::new(u32::MAX as u64 + 2, 1, 0);
    }

    #[test]
    fn allocate_until_full_then_free() {
        let mut a = Allocator::new(640, 64, 0);
        assert_eq!(a.num_segments(), 10);
        let mut seen = HashSet::new();
        for _ in 0..10 {
            let s = a.allocate().expect("should allocate");
            assert!(seen.insert(s), "segments must be unique: {s:?}");
            assert!(!a.is_free(s));
        }
        assert_eq!(a.free_segments(), 0);
        assert!(a.allocate().is_none(), "device is full");

        let victim = SegmentId(3);
        a.free(victim).unwrap();
        assert!(a.is_free(victim));
        assert_eq!(a.free_segments(), 1);
        assert_eq!(a.allocate(), Some(victim));
        assert_eq!(a.free_segments(), 0);
    }

    #[test]
    fn double_free_and_out_of_range() {
        let mut a = Allocator::new(640, 64, 0);
        let s = a.allocate().unwrap();
        a.free(s).unwrap();
        assert_eq!(a.free(s), Err(AllocError::AlreadyFree(s.0)));
        assert_eq!(
            a.free(SegmentId(99)),
            Err(AllocError::OutOfRange {
                seg: 99,
                num_segments: 10
            })
        );
    }

    #[test]
    fn mark_allocated_for_recovery() {
        let mut a = Allocator::new(640, 64, 0);
        a.mark_allocated(SegmentId(0)).unwrap();
        a.mark_allocated(SegmentId(5)).unwrap();
        assert_eq!(a.used_segments(), 2);
        assert!(!a.is_free(SegmentId(0)));
        assert!(!a.is_free(SegmentId(5)));
        assert_eq!(
            a.mark_allocated(SegmentId(0)),
            Err(AllocError::AlreadyUsed(0))
        );
        for _ in 0..8 {
            let s = a.allocate().unwrap();
            assert_ne!(s, SegmentId(0));
            assert_ne!(s, SegmentId(5));
        }
        assert!(a.allocate().is_none());
    }

    #[test]
    fn non_multiple_of_64_segments_has_no_phantom() {
        let mut a = Allocator::new(1000, 64, 100);
        let mut count = 0;
        while let Some(s) = a.allocate() {
            assert!(s.0 < 14, "allocated phantom segment {s:?}");
            count += 1;
        }
        assert_eq!(count, 14, "must allocate exactly the valid segments");
    }

    #[test]
    fn tiny_device_yields_no_segments() {
        let mut a = Allocator::new(10, 64, 0);
        assert_eq!(a.num_segments(), 0);
        assert!(a.allocate().is_none());
    }

    #[test]
    fn data_reserve_gates_only_the_data_path() {
        let mut a = Allocator::new(640, 64, 0);
        assert_eq!(a.num_segments(), 10);
        a.set_data_reserve(3);
        assert_eq!(a.maint_reserve(), 3);

        let mut data = Vec::new();
        while let Some(s) = a.allocate_data() {
            data.push(s);
        }
        assert_eq!(data.len(), 7, "data path stops with `reserve` free segments");
        assert_eq!(a.free_segments(), 3);
        assert!(a.allocate_data().is_none(), "data path refuses at the floor");

        assert!(a.allocate().is_some());
        assert!(a.allocate().is_some());
        assert!(a.allocate().is_some());
        assert_eq!(a.free_segments(), 0);
        assert!(a.allocate().is_none(), "truly full now");
    }

    #[test]
    fn data_reserve_zero_is_transparent() {
        let mut a = Allocator::new(640, 64, 0);
        assert_eq!(a.maint_reserve(), 0);
        let mut n = 0;
        while a.allocate_data().is_some() {
            n += 1;
        }
        assert_eq!(n, 10);
        assert_eq!(a.free_segments(), 0);
    }

    #[test]
    fn data_reserve_is_clamped_to_num_segments() {
        let mut a = Allocator::new(640, 64, 0);
        a.set_data_reserve(1_000_000);
        assert_eq!(a.maint_reserve(), 10, "reserve clamped to num_segments");
        assert!(a.allocate_data().is_none());
        assert!(a.allocate().is_some());
    }
}
