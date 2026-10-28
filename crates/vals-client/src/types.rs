use crate::hlc::Version;

#[doc(hidden)]
pub const KIND_PUT: u8 = 0;

#[doc(hidden)]
pub fn split_stored_blob(blob: &[u8]) -> (bool, &[u8]) {
    match blob.split_first() {
        Some((&kind, rest)) => (kind == KIND_PUT, rest),
        None => (false, &[]),
    }
}

pub type EntryPage = (Vec<(Vec<u8>, Stored)>, bool);

pub type DigestPage = (Vec<(Vec<u8>, Version)>, bool);
pub type DigestCursorPage = (Vec<(Vec<u8>, Version)>, Option<Vec<u8>>);

/// A key's value and version as read.
///
/// Reads and scans of the [`Coordinator`](crate::Coordinator) return only
/// live entries, so `value` is `Some` there; `None` marks a delete
/// (tombstone) in lower-level replies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stored {
    /// The version of this value.
    pub version: Version,
    /// The value, or `None` for a delete.
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

#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Applied {
    Won,
    Lost(Version),
    Rejected,
}

/// The result of [`Coordinator::cas`](crate::Coordinator::cas).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CasResult {
    /// The check passed and the write was acknowledged at this version.
    Applied(Version),
    /// The key's version did not match the expected one; nothing was written.
    Mismatch {
        /// The version found, which is a delete's version if the key was
        /// deleted (`None` = no stored version).
        current: Option<Version>,
    },
}
