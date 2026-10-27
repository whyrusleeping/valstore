use crate::coord::TransportError;
use crate::ring::NodeId;
use crate::types::{DigestCursorPage, EntryPage};
use crate::wire::RtFragment;
use crate::zone::ZoneId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtSyncPage {
    pub same: bool,
    pub more: bool,
    pub fragments: Vec<RtFragment>,
}

pub trait AePeer {
    fn tree(
        &self,
        zone_id: ZoneId,
        level: u32,
        scope_g: u32,
        scope: &[u32],
        requester: Option<NodeId>,
    ) -> impl std::future::Future<Output = Result<Vec<(u32, u64)>, TransportError>>;

    fn bucket_digests(
        &self,
        zone_id: ZoneId,
        num_buckets: u32,
        ids: &[u32],
        after: Option<&[u8]>,
    ) -> impl std::future::Future<Output = Result<DigestCursorPage, TransportError>>;

    fn values(
        &self,
        zone_id: ZoneId,
        keys: &[Vec<u8>],
    ) -> impl std::future::Future<Output = Result<EntryPage, TransportError>>;

    fn range_tombstones(
        &self,
        zone_id: ZoneId,
        requester: Option<NodeId>,
        digest: u64,
        after: Option<&[u8]>,
    ) -> impl std::future::Future<Output = Result<Option<RtSyncPage>, TransportError>> {
        let _ = (zone_id, requester, digest, after);
        std::future::ready(Ok(None))
    }
}
