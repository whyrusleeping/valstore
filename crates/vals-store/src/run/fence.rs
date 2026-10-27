#[derive(Debug, PartialEq, Eq)]
struct Block {
    off: u64,
    len: u32,
    key_end: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct FenceIndex {
    keys: Box<[u8]>,
    blocks: Box<[Block]>,
    max_hlc: Box<[u64]>,
}

impl FenceIndex {
    pub(super) fn len(&self) -> usize {
        self.blocks.len()
    }

    pub(super) fn key(&self, i: usize) -> &[u8] {
        let start = if i == 0 { 0 } else { self.blocks[i - 1].key_end as usize };
        &self.keys[start..self.blocks[i].key_end as usize]
    }

    pub(super) fn get(&self, i: usize) -> Option<(&[u8], &u64, &u32)> {
        self.blocks.get(i).map(|b| (self.key(i), &b.off, &b.len))
    }

    pub(super) fn block_ref(&self, i: usize) -> (u64, u32) {
        let b = &self.blocks[i];
        (b.off, b.len)
    }

    pub(super) fn last(&self) -> Option<(&[u8], &u64, &u32)> {
        self.len().checked_sub(1).and_then(|i| self.get(i))
    }

    pub(super) fn iter(&self) -> impl ExactSizeIterator<Item = (&[u8], &u64, &u32)> {
        (0..self.len()).map(|i| self.get(i).unwrap())
    }

    pub(super) fn partition_point(&self, mut pred: impl FnMut(&[u8]) -> bool) -> usize {
        let mut left = 0;
        let mut right = self.len();
        while left < right {
            let mid = left + (right - left) / 2;
            if pred(self.key(mid)) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        left
    }

    pub(super) fn has_max_hlc(&self) -> bool {
        self.max_hlc.len() == self.blocks.len()
    }

    pub(super) fn max_hlc(&self, i: usize) -> Option<u64> {
        self.max_hlc.get(i).copied()
    }

    pub(super) fn max_block_len(&self) -> usize {
        self.blocks.iter().map(|b| b.len as usize).max().unwrap_or(0)
    }

    pub(super) fn allocated_bytes(&self) -> usize {
        self.keys.len()
            + self.blocks.len() * std::mem::size_of::<Block>()
            + self.max_hlc.len() * std::mem::size_of::<u64>()
    }
}

#[derive(Default)]
pub(super) struct FenceBuilder {
    keys: Vec<u8>,
    blocks: Vec<Block>,
    max_hlc: Vec<u64>,
}

pub(super) fn key_end(start: usize, len: usize) -> Result<u32, &'static str> {
    start.checked_add(len).and_then(|end| u32::try_from(end).ok())
        .ok_or("index key bytes overflow")
}

impl FenceBuilder {
    pub(super) fn with_capacity(n: usize, key_bytes: usize) -> Self {
        Self { keys: Vec::with_capacity(key_bytes), blocks: Vec::with_capacity(n), max_hlc: Vec::new() }
    }

    pub(super) fn push(&mut self, key: &[u8], off: u64, len: u32) -> Result<(), &'static str> {
        let key_end = key_end(self.keys.len(), key.len())?;
        self.keys.extend_from_slice(key);
        self.blocks.push(Block { off, len, key_end });
        Ok(())
    }

    pub(super) fn push_with_max_hlc(&mut self, key: &[u8], off: u64, len: u32, max_hlc: u64) -> Result<(), &'static str> {
        debug_assert_eq!(self.max_hlc.len(), self.blocks.len(), "every block of a v6 index carries max_hlc");
        self.push(key, off, len)?;
        self.max_hlc.push(max_hlc);
        Ok(())
    }

    pub(super) fn allocated_bytes(&self) -> usize {
        self.keys.capacity()
            + self.blocks.capacity() * std::mem::size_of::<Block>()
            + self.max_hlc.capacity() * std::mem::size_of::<u64>()
    }

    pub(super) fn finish(self) -> FenceIndex {
        FenceIndex {
            keys: self.keys.into_boxed_slice(),
            blocks: self.blocks.into_boxed_slice(),
            max_hlc: self.max_hlc.into_boxed_slice(),
        }
    }
}
