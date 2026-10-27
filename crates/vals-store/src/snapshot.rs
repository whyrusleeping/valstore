use crate::vlog::ValuePtr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotEntry {
    pub key: Vec<u8>,
    pub ptr: ValuePtr,
    pub seq: u64,
    pub version: u128,
    pub tombstone: bool,
}
