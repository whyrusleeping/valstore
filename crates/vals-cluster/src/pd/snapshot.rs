use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PdStateView {
    pub incarnation: Option<u128>,
    pub lineage: Option<Lineage>,
    pub members: AddressedMembers,
    pub zones: Catalog,
    pub range_maps: HashMap<ZoneId, RangeMap>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Lineage {
    pub predecessor: u128,
    pub backup_id: u128,
    pub source_applied: u64,
}

const LINEAGE_VALUE_VERSION: u8 = 1;

fn lineage_key() -> Vec<u8> {
    vec![b'L']
}

fn encode_lineage(lineage: &Lineage) -> Vec<u8> {
    let mut value = Vec::with_capacity(41);
    value.push(LINEAGE_VALUE_VERSION);
    value.extend_from_slice(&lineage.predecessor.to_le_bytes());
    value.extend_from_slice(&lineage.backup_id.to_le_bytes());
    value.extend_from_slice(&lineage.source_applied.to_le_bytes());
    value
}

fn decode_lineage(value: &[u8]) -> Option<Lineage> {
    if value.len() != 41 || value[0] != LINEAGE_VALUE_VERSION {
        return None;
    }
    Some(Lineage {
        predecessor: u128::from_le_bytes(value[1..17].try_into().ok()?),
        backup_id: u128::from_le_bytes(value[17..33].try_into().ok()?),
        source_applied: u64::from_le_bytes(value[33..41].try_into().ok()?),
    })
}

pub fn view_state(entries: &[(Vec<u8>, Vec<u8>)]) -> PdStateView {
    let map: BTreeMap<&[u8], &[u8]> =
        entries.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
    let keys: Vec<Vec<u8>> = entries.iter().map(|(k, _)| k.clone()).collect();
    let get = |key: &[u8]| map.get(key).map(|v| v.to_vec());
    PdStateView {
        incarnation: incarnation_of(&get),
        lineage: get(&lineage_key()).and_then(|value| decode_lineage(&value)),
        members: members_of(&keys, &get),
        zones: zones_of(&keys, &get),
        range_maps: range_maps_of(&keys, &get),
    }
}

pub fn without_pending(map: &RangeMap) -> RangeMap {
    let mut cleared = map.clone();
    for index in 0..cleared.num_ranges() {
        cleared.clear_pending(index);
    }
    cleared
}

fn member_weights(members: &AddressedMembers) -> Vec<(NodeId, u32)> {
    let mut weights: Vec<(NodeId, u32)> = members.iter().map(|(node, weight, _)| (*node, *weight)).collect();
    weights.sort_unstable();
    weights
}

type StateEntries = Vec<(Vec<u8>, Vec<u8>)>;

pub fn restored_state(
    entries: &[(Vec<u8>, Vec<u8>)],
    members: &AddressedMembers,
    incarnation: u128,
    lineage: Lineage,
) -> Result<StateEntries, String> {
    let source = view_state(entries);
    if incarnation == 0 || source.incarnation == Some(incarnation) || lineage.predecessor == incarnation {
        return Err("a restored PD needs a new non-zero cluster incarnation".into());
    }
    if member_weights(&source.members) != member_weights(members) {
        return Err(format!(
            "the target data nodes (id, weight) {:?} differ from the backup's members {:?}; \
             a restore keeps node ids and weights so placement is unchanged",
            member_weights(members),
            member_weights(&source.members),
        ));
    }
    if let Some((node, _, _)) = members.iter().find(|(_, _, addrs)| addrs.is_empty()) {
        return Err(format!("target data node {node} has no data address"));
    }
    let skip = [balance_lock_key(), incarnation_key(), lineage_key()];
    let mut out = BTreeMap::new();
    for (key, value) in entries {
        if skip.contains(key) || pd_peer_id_of_key(key).is_some() {
            continue;
        }
        let value = if let Ok(id) = <[u8; 4]>::try_from(key.as_slice()) {
            let node = u32::from_le_bytes(id);
            let (_, weight, addrs) = members
                .iter()
                .find(|(member, _, _)| *member == node)
                .ok_or_else(|| format!("backup member {node} is not a target data node"))?;
            encode_member_value(*weight, addrs)
        } else if let Some((zone_id, _)) = range_record_of_key(key) {
            let (token, _, debt) = decode_range_record(value)
                .ok_or_else(|| format!("backup PD state holds an undecodable range record of zone {zone_id}"))?;
            encode_range_record(&token, None, debt)
        } else if let Some(zone_id) = range_map_id_of_key(key) {
            let map = RangeMap::decode(value)
                .ok_or_else(|| format!("backup PD state holds an undecodable range map of zone {zone_id}"))?;
            without_pending(&map).encode()
        } else {
            value.clone()
        };
        out.insert(key.clone(), value);
    }
    out.insert(incarnation_key(), encode_incarnation(incarnation));
    out.insert(lineage_key(), encode_lineage(&lineage));
    Ok(out.into_iter().collect())
}

type Interval<'a> = (&'a [u8], &'a [u8], Option<&'a [u8]>);

fn placement_intervals(map: &RangeMap) -> Vec<Interval<'_>> {
    let mut out: Vec<Interval<'_>> = Vec::new();
    for index in 0..map.num_ranges() {
        let lower = map.range_bounds(index).0.unwrap_or_default();
        let owner = (map.token_of(index), map.pending_of(index));
        if out.last().is_some_and(|&(_, token, pending)| (token, pending) == owner) {
            continue;
        }
        out.push((lower, owner.0, owner.1));
    }
    out
}

pub fn placement_equivalent(a: &RangeMap, b: &RangeMap) -> bool {
    a.supports_opaque_tokens() == b.supports_opaque_tokens()
        && placement_intervals(a) == placement_intervals(b)
}
