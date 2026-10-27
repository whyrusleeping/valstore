use super::*;

const KIND_LEN: usize = 1;
pub(super) const KIND_PUT: u8 = 0;
pub(super) const KIND_TOMB: u8 = 1;

static BLIND_APPEND: OnceLock<bool> = OnceLock::new();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ApplyMode {
    Legacy,
    Blind,
}

pub fn configure_blind_append(enabled: bool) -> Result<(), bool> {
    match BLIND_APPEND.set(enabled) {
        Ok(()) => Ok(()),
        Err(requested) if BLIND_APPEND.get() == Some(&requested) => Ok(()),
        Err(_) => Err(*BLIND_APPEND.get().unwrap()),
    }
}

pub fn blind_append_enabled() -> bool {
    *BLIND_APPEND.get().unwrap_or(&false) && vals_store::winner_directory_operational()
}

pub(crate) fn unconditional_apply_mode() -> ApplyMode {
    if blind_append_enabled() {
        ApplyMode::Blind
    } else {
        ApplyMode::Legacy
    }
}

pub(crate) fn pack_version(v: Version) -> u128 {
    ((v.hlc as u128) << 64) | (v.node as u128)
}

pub(crate) fn unpack_version(v: u128) -> Version {
    Version {
        hlc: (v >> 64) as u64,
        node: v as u64,
    }
}

pub(super) fn encode(kind: u8, value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(KIND_LEN + value.len());
    buf.push(kind);
    buf.extend_from_slice(value);
    buf
}

pub(super) fn decode(bytes: &[u8]) -> (u8, &[u8]) {
    (bytes[0], &bytes[KIND_LEN..])
}

pub use vals_client::types::split_stored_blob;

pub(crate) fn decode_read(record_version: u128, blob: &[u8]) -> (Version, Option<&[u8]>) {
    let (kind, value) = decode(blob);
    (
        unpack_version(record_version),
        (kind == KIND_PUT).then_some(value),
    )
}

pub(crate) fn adopt_blob(blob: Vec<u8>) -> Option<Vec<u8>> {
    (blob.first() == Some(&KIND_PUT)).then(|| {
        let mut v = blob;
        v.drain(..KIND_LEN);
        v
    })
}

pub async fn execute_read_plan<D: AsyncRead + BlockDevice>(
    plan: &vals_store::ReadPlan,
    dev: &D,
) -> Result<Stored, EngineError> {
    Ok(stored_from_record(plan.execute(dev).await?))
}

pub(crate) async fn execute_read_plan_checked<D: AsyncRead + BlockDevice>(
    plan: &vals_store::ReadPlan,
    dev: &D,
    expected_key: &[u8],
) -> Result<Stored, vals_store::vlog::LogError> {
    Ok(stored_from_record(plan.execute_checked(dev, expected_key).await?))
}

fn stored_from_record(rec: vals_store::Record) -> Stored {
    let version = unpack_version(rec.version);
    let kind = rec.value[0];
    let value = (kind == KIND_PUT).then(|| {
        let mut v = rec.value;
        v.drain(..KIND_LEN);
        v
    });
    Stored { version, value }
}

pub use vals_client::types::{Applied, CasResult, DigestCursorPage, DigestPage, EntryPage, Stored};

pub enum ValuePlan {
    Read(vals_store::ReadPlan),
    Covered(Version),
}

pub type PlanPage = (Vec<(Vec<u8>, ValuePlan)>, bool);

pub type SpanPage = (Vec<(Vec<u8>, Version)>, vals_store::SpanReadPlan, bool);

pub type PlacementFn = std::sync::Arc<dyn Fn(&[u8]) -> Vec<NodeId> + Send + Sync>;

