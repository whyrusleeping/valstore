use crate::hlc::Version;

#[doc(hidden)]
pub const KIND_PUT: u8 = 0;

pub fn split_stored_blob(blob: &[u8]) -> (bool, &[u8]) {
    match blob.split_first() {
        Some((&kind, rest)) => (kind == KIND_PUT, rest),
        None => (false, &[]),
    }
}

pub type EntryPage = (Vec<(Vec<u8>, Stored)>, bool);

pub type DigestPage = (Vec<(Vec<u8>, Version)>, bool);
pub type DigestCursorPage = (Vec<(Vec<u8>, Version)>, Option<Vec<u8>>);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stored {
    pub version: Version,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MixedReadRole {
    Value,
    Digest,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MixedReadEntry<'a> {
    pub key: &'a [u8],
    pub role: MixedReadRole,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MixedReadReply {
    Value(Option<Stored>),
    Digest(Option<Version>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Applied {
    Won,
    Lost(Version),
    Rejected,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CasResult {
    Applied(Version),
    Mismatch {
        current: Option<Version>,
    },
}
