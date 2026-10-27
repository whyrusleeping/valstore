use crate::{BlockError, Result};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub const DSM_MAX_RANGES: usize = 256;
pub const DSM_MAX_SECTORS: u32 = 1 << 31;

#[derive(Clone, Copy, Debug, Default)]
pub struct DeviceDeallocTotals {
    pub commands: u64,
    pub ranges: u64,
    pub bytes: u64,
    pub errors: u64,
    pub unsupported: u64,
    pub latency_us: u64,
}

static COMMANDS: AtomicU64 = AtomicU64::new(0);
static RANGES: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static ERRORS: AtomicU64 = AtomicU64::new(0);
static UNSUPPORTED: AtomicU64 = AtomicU64::new(0);
static LATENCY_US: AtomicU64 = AtomicU64::new(0);

pub fn device_dealloc_totals() -> DeviceDeallocTotals {
    DeviceDeallocTotals {
        commands: COMMANDS.load(Ordering::Relaxed),
        ranges: RANGES.load(Ordering::Relaxed),
        bytes: BYTES.load(Ordering::Relaxed),
        errors: ERRORS.load(Ordering::Relaxed),
        unsupported: UNSUPPORTED.load(Ordering::Relaxed),
        latency_us: LATENCY_US.load(Ordering::Relaxed),
    }
}

pub(crate) fn note_dealloc(start: Instant, ranges: usize, bytes: u64, result: &Result<bool>) {
    COMMANDS.fetch_add(1, Ordering::Relaxed);
    RANGES.fetch_add(ranges as u64, Ordering::Relaxed);
    LATENCY_US.fetch_add(start.elapsed().as_micros() as u64, Ordering::Relaxed);
    match result {
        Ok(true) => { BYTES.fetch_add(bytes, Ordering::Relaxed); }
        Ok(false) => { UNSUPPORTED.fetch_add(1, Ordering::Relaxed); }
        Err(_) => { ERRORS.fetch_add(1, Ordering::Relaxed); }
    }
}

pub fn check_dealloc_ranges(num_blocks: u64, ranges: &[(u64, u64)]) -> Result<()> {
    for &(lba, blocks) in ranges {
        if lba.checked_add(blocks).is_none_or(|end| end > num_blocks) {
            return Err(BlockError::OutOfRange { lba, blocks, num_blocks });
        }
    }
    Ok(())
}

pub fn dealloc_commands(
    ranges: &[(u64, u64)],
    base_block: u64,
    sectors_per_block: u64,
    max_sectors: u32,
) -> Result<Vec<Vec<(u64, u32)>>> {
    assert!(sectors_per_block > 0 && max_sectors > 0);
    let invalid = || BlockError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput, "invalid deallocation geometry or unsorted ranges",
    ));
    let mut merged: Vec<(u64, u64)> = Vec::new();
    for &(lba, blocks) in ranges {
        if blocks == 0 { continue; }
        let start = base_block.checked_add(lba).and_then(|b| b.checked_mul(sectors_per_block)).ok_or_else(invalid)?;
        let count = blocks.checked_mul(sectors_per_block).ok_or_else(invalid)?;
        start.checked_add(count).ok_or_else(invalid)?;
        if let Some((prev, len)) = merged.last_mut() {
            let end = *prev + *len;
            if start < end { return Err(invalid()); }
            if start == end {
                *len = len.checked_add(count).ok_or_else(invalid)?;
                continue;
            }
        }
        merged.push((start, count));
    }
    let mut commands = Vec::new();
    let mut command = Vec::new();
    for (mut start, mut count) in merged {
        while count > 0 {
            let take = count.min(max_sectors as u64) as u32;
            command.push((start, take));
            start += take as u64;
            count -= take as u64;
            if command.len() == DSM_MAX_RANGES {
                commands.push(std::mem::take(&mut command));
            }
        }
    }
    if !command.is_empty() { commands.push(command); }
    Ok(commands)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coalesces_splits_and_converts_sectors() {
        assert_eq!(dealloc_commands(&[(2, 2), (4, 3), (9, 1)], 10, 8, 24).unwrap(),
            vec![vec![(96, 24), (120, 16), (152, 8)]]);
        assert!(dealloc_commands(&[], 0, 8, DSM_MAX_SECTORS).unwrap().is_empty());
        assert!(dealloc_commands(&[(0, 0)], 0, 1, 1).unwrap().is_empty());
        assert!(dealloc_commands(&[(2, 2), (1, 1)], 0, 1, 1).is_err());
        assert!(dealloc_commands(&[(u64::MAX, 1)], 0, 1, 1).is_err());
    }

    #[test]
    fn bounds_command_ranges_and_sector_counts() {
        let ranges: Vec<_> = (0..600).map(|i| (i * 2, 1)).collect();
        let commands = dealloc_commands(&ranges, 0, 1, DSM_MAX_SECTORS).unwrap();
        assert_eq!(commands.iter().map(Vec::len).collect::<Vec<_>>(), [256, 256, 88]);
        assert_eq!(dealloc_commands(&[(0, DSM_MAX_SECTORS as u64 + 1)], 0, 1, DSM_MAX_SECTORS).unwrap(),
            vec![vec![(0, DSM_MAX_SECTORS), (DSM_MAX_SECTORS as u64, 1)]]);
    }
}
