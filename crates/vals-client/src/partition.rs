use std::collections::BTreeSet;
use std::sync::{Arc, OnceLock};

use crate::ring::{hash64, NodeId, Ring};
use crate::zone::ZoneId;

const RANGE_MAP_V2_MAGIC: u32 = u32::MAX;
const RANGE_MAP_V2_VERSION: u32 = 2;
const RANGE_MAP_V3_VERSION: u32 = 3;
const RANGE_MAP_V4_VERSION: u32 = 4;
pub const OPAQUE_TOKEN_MAGIC: [u8; 4] = *b"VOPT";
pub const OPAQUE_TOKEN_VERSION: u8 = 1;
pub const OPAQUE_TOKEN_SEED_LEN: usize = 16;
pub const OPAQUE_TOKEN_LEN: usize = OPAQUE_TOKEN_MAGIC.len() + 1 + OPAQUE_TOKEN_SEED_LEN;

pub fn opaque_placement_token(seed: [u8; OPAQUE_TOKEN_SEED_LEN]) -> Vec<u8> {
    let mut token = Vec::with_capacity(OPAQUE_TOKEN_LEN);
    token.extend_from_slice(&OPAQUE_TOKEN_MAGIC);
    token.push(OPAQUE_TOKEN_VERSION);
    token.extend_from_slice(&seed);
    token
}

#[derive(Clone)]
pub struct RangeMap {
    parts: Arc<RangeMapParts>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RangeMapParts {
    splits: Vec<Vec<u8>>,
    tokens: Vec<Vec<u8>>,
    pending: Vec<Option<Vec<u8>>>,
    placement_debt: Vec<bool>,
    opaque_tokens: bool,
    index: SplitIndexCell,
}

impl PartialEq for RangeMap {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.parts, &other.parts) || self.parts == other.parts
    }
}

impl Eq for RangeMap {}

impl std::fmt::Debug for RangeMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let p = &*self.parts;
        f.debug_struct("RangeMap")
            .field("splits", &p.splits)
            .field("tokens", &p.tokens)
            .field("pending", &p.pending)
            .field("placement_debt", &p.placement_debt)
            .field("opaque_tokens", &p.opaque_tokens)
            .finish()
    }
}

const SPLIT_INDEX_FANOUT: usize = 16;

#[derive(Clone, Default)]
struct SplitIndexCell(OnceLock<SplitIndex>);

impl PartialEq for SplitIndexCell {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Eq for SplitIndexCell {}

impl std::fmt::Debug for SplitIndexCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SplitIndex")
    }
}

#[derive(Clone)]
struct SplitIndexLevel {
    stride: usize,
    entries: usize,
    prefix: Vec<u32>,
    heads: Vec<u64>,
}

#[derive(Clone)]
struct SplitIndex {
    levels: Vec<SplitIndexLevel>,
}

fn split_head(key: &[u8], prefix: usize) -> u64 {
    let tail = key.get(prefix..).unwrap_or_default();
    let tail = &tail[..tail.len().min(8)];
    let mut buf = [0u8; 8];
    buf[..tail.len()].copy_from_slice(tail);
    u64::from_be_bytes(buf)
}

fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

impl SplitIndex {
    fn build(splits: &[Vec<u8>]) -> Self {
        let mut levels = Vec::new();
        let mut stride = 1;
        let mut entries = splits.len();
        while entries > SPLIT_INDEX_FANOUT {
            let entry = |j: usize| splits[j * stride].as_slice();
            let blocks = entries.div_ceil(SPLIT_INDEX_FANOUT);
            let mut prefix = Vec::with_capacity(blocks);
            let mut heads = Vec::with_capacity(entries);
            for block in 0..blocks {
                let start = block * SPLIT_INDEX_FANOUT;
                let end = (start + SPLIT_INDEX_FANOUT).min(entries);
                let fence = if end < entries { entry(end) } else { entry(end - 1) };
                let p = common_prefix(entry(start), fence);
                prefix.push(p as u32);
                heads.extend((start..end).map(|j| split_head(entry(j), p)));
            }
            levels.push(SplitIndexLevel { stride, entries, prefix, heads });
            stride *= SPLIT_INDEX_FANOUT;
            entries = blocks;
        }
        SplitIndex { levels }
    }

    fn count_le(&self, splits: &[Vec<u8>], key: &[u8]) -> usize {
        let stride = self.levels.last().map_or(1, |l| l.stride * SPLIT_INDEX_FANOUT);
        let top = splits.len().div_ceil(stride);
        let mut count = partition_le(0, top, |j| splits[j * stride].as_slice() <= key);
        for level in self.levels.iter().rev() {
            if count == 0 {
                return 0;
            }
            let block = count - 1;
            let start = block * SPLIT_INDEX_FANOUT;
            let end = (start + SPLIT_INDEX_FANOUT).min(level.entries);
            let entry = |j: usize| splits[j * level.stride].as_slice();
            let p = level.prefix[block] as usize;
            if end == level.entries && !key.starts_with(&entry(start)[..p]) {
                count = end;
                continue;
            }
            let head = split_head(key, p);
            let heads = &level.heads[start..end];
            let below = heads.iter().map(|&h| usize::from(h < head)).sum::<usize>();
            let at_most = heads.iter().map(|&h| usize::from(h <= head)).sum::<usize>();
            count = partition_le(start + below, start + at_most, |j| entry(j) <= key);
        }
        count
    }
}

fn partition_le(mut lo: usize, mut hi: usize, le: impl Fn(usize) -> bool) -> usize {
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if le(mid) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidRangeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeSlice {
    pub index: usize,
    pub token: Vec<u8>,
    pub lo: Option<Vec<u8>>,
    pub hi: Option<Vec<u8>>,
}

impl RangeMap {
    fn from_validated(parts: RangeMapParts) -> Self {
        RangeMap { parts: Arc::new(parts) }
    }

    fn parts_mut(&mut self) -> &mut RangeMapParts {
        Arc::make_mut(&mut self.parts)
    }

    pub fn single() -> Self {
        RangeMap::from_validated(RangeMapParts {
            splits: Vec::new(),
            tokens: vec![Vec::new()],
            pending: vec![None],
            placement_debt: vec![false],
            opaque_tokens: false,
            index: SplitIndexCell::default(),
        })
    }

    pub fn from_splits(splits: impl IntoIterator<Item = Vec<u8>>) -> Self {
        let set: BTreeSet<Vec<u8>> = splits.into_iter().filter(|s| !s.is_empty()).collect();
        let splits: Vec<Vec<u8>> = set.into_iter().collect();
        let tokens: Vec<Vec<u8>> = std::iter::once(Vec::new())
            .chain(splits.iter().cloned())
            .collect();
        let pending = vec![None; tokens.len()];
        let placement_debt = vec![false; tokens.len()];
        RangeMap::from_validated(RangeMapParts {
            splits,
            tokens,
            pending,
            placement_debt,
            opaque_tokens: false,
            index: SplitIndexCell::default(),
        })
    }

    pub fn from_splits_and_tokens(
        splits: Vec<Vec<u8>>,
        tokens: Vec<Vec<u8>>,
    ) -> Result<Self, InvalidRangeMap> {
        let pending = vec![None; tokens.len()];
        Self::from_splits_tokens_and_pending(splits, tokens, pending)
    }

    pub fn from_splits_tokens_and_pending(
        splits: Vec<Vec<u8>>,
        tokens: Vec<Vec<u8>>,
        pending: Vec<Option<Vec<u8>>>,
    ) -> Result<Self, InvalidRangeMap> {
        let placement_debt = legacy_placement_debt(&splits, &tokens);
        Self::from_parts(splits, tokens, pending, placement_debt, false)
    }

    pub fn from_opaque_parts(
        splits: Vec<Vec<u8>>,
        tokens: Vec<Vec<u8>>,
        pending: Vec<Option<Vec<u8>>>,
        placement_debt: Vec<bool>,
    ) -> Result<Self, InvalidRangeMap> {
        Self::from_parts(splits, tokens, pending, placement_debt, true)
    }

    fn from_parts(
        splits: Vec<Vec<u8>>,
        tokens: Vec<Vec<u8>>,
        pending: Vec<Option<Vec<u8>>>,
        placement_debt: Vec<bool>,
        opaque_tokens: bool,
    ) -> Result<Self, InvalidRangeMap> {
        if tokens.len() != splits.len() + 1
            || pending.len() != tokens.len()
            || placement_debt.len() != tokens.len()
            || splits.iter().any(|s| s.is_empty())
            || splits.windows(2).any(|w| w[0] >= w[1])
        {
            return Err(InvalidRangeMap);
        }
        for (i, token) in tokens.iter().enumerate() {
            let lower = if i == 0 { b"".as_slice() } else { &splits[i - 1] };
            if !valid_token(&splits, lower, token, opaque_tokens) {
                return Err(InvalidRangeMap);
            }
            if let Some(target) = &pending[i] {
                if !valid_token(&splits, lower, target, opaque_tokens) || target == token {
                    return Err(InvalidRangeMap);
                }
            }
        }
        Ok(RangeMap::from_validated(RangeMapParts {
            splits,
            tokens,
            pending,
            placement_debt,
            opaque_tokens,
            index: SplitIndexCell::default(),
        }))
    }

    pub fn splits(&self) -> &[Vec<u8>] {
        &self.parts.splits
    }

    pub fn tokens(&self) -> &[Vec<u8>] {
        &self.parts.tokens
    }

    pub fn pending(&self) -> &[Option<Vec<u8>>] {
        &self.parts.pending
    }

    pub fn placement_debt(&self) -> &[bool] {
        &self.parts.placement_debt
    }

    pub fn placement_debt_of(&self, index: usize) -> bool {
        assert!(index < self.num_ranges(), "range index out of bounds");
        self.parts.placement_debt[index]
    }

    pub fn supports_opaque_tokens(&self) -> bool {
        self.parts.opaque_tokens
    }

    pub fn enable_opaque_tokens(&mut self) {
        if !self.parts.opaque_tokens {
            self.parts_mut().opaque_tokens = true;
        }
    }

    pub fn pending_of(&self, index: usize) -> Option<&[u8]> {
        assert!(index < self.num_ranges(), "range index out of bounds");
        self.parts.pending[index].as_deref()
    }

    pub fn pending_for_key(&self, key: &[u8]) -> Option<&[u8]> {
        self.pending_of(self.range_index(key))
    }

    pub fn set_pending(&mut self, index: usize, token: Vec<u8>) -> bool {
        assert!(index < self.num_ranges(), "range index out of bounds");
        let p = &*self.parts;
        let lower = if index == 0 { b"".as_slice() } else { &p.splits[index - 1] };
        if token == p.tokens[index] || !valid_token(&p.splits, lower, &token, p.opaque_tokens) {
            return false;
        }
        self.parts_mut().pending[index] = Some(token);
        true
    }

    pub fn set_opaque_pending(
        &mut self,
        index: usize,
        seed: [u8; OPAQUE_TOKEN_SEED_LEN],
    ) -> bool {
        if !self.parts.opaque_tokens {
            return false;
        }
        self.set_pending(index, opaque_placement_token(seed))
    }

    pub fn clear_pending(&mut self, index: usize) -> bool {
        assert!(index < self.num_ranges(), "range index out of bounds");
        if self.parts.pending[index].is_none() {
            return false;
        }
        self.parts_mut().pending[index] = None;
        true
    }

    pub fn commit_pending(&mut self, index: usize) -> bool {
        assert!(index < self.num_ranges(), "range index out of bounds");
        if self.parts.pending[index].is_none() {
            return false;
        }
        let p = self.parts_mut();
        let Some(token) = p.pending[index].take() else { return false };
        p.tokens[index] = token;
        if p.opaque_tokens {
            p.placement_debt[index] = false;
        } else {
            p.placement_debt = legacy_placement_debt(&p.splits, &p.tokens);
        }
        true
    }

    pub fn active_rehomes(&self) -> usize {
        self.parts.pending.iter().filter(|p| p.is_some()).count()
    }

    pub fn is_canonical(&self) -> bool {
        self.unrehomed_ranges() == 0
    }

    pub fn unrehomed_ranges(&self) -> usize {
        self.parts.placement_debt.iter().filter(|&&debt| debt).count()
    }

    pub fn num_ranges(&self) -> usize {
        self.parts.splits.len() + 1
    }

    pub fn range_index(&self, key: &[u8]) -> usize {
        let p = &*self.parts;
        if p.splits.len() <= SPLIT_INDEX_FANOUT {
            return p.splits.partition_point(|s| s.as_slice() <= key);
        }
        p.index
            .0
            .get_or_init(|| SplitIndex::build(&p.splits))
            .count_le(&p.splits, key)
    }

    pub fn range_token(&self, key: &[u8]) -> &[u8] {
        &self.parts.tokens[self.range_index(key)]
    }

    pub fn token_of(&self, index: usize) -> &[u8] {
        assert!(index < self.num_ranges(), "range index out of bounds");
        &self.parts.tokens[index]
    }

    pub fn range_bounds(&self, index: usize) -> (Option<&[u8]>, Option<&[u8]>) {
        assert!(index < self.num_ranges(), "range index out of bounds");
        let splits = &self.parts.splits;
        let lo = (index > 0).then(|| splits[index - 1].as_slice());
        let hi = splits.get(index).map(|s| s.as_slice());
        (lo, hi)
    }

    pub fn replicas_for_key(
        &self,
        ring: &Ring,
        zone_id: ZoneId,
        key: &[u8],
        rf: usize,
        scope: Option<&BTreeSet<NodeId>>,
    ) -> Vec<NodeId> {
        ring.replicas_in_zone(zone_id, self.range_token(key), rf, scope)
    }

    pub fn replicas_for_key_while_rehoming(
        &self,
        ring: &Ring,
        zone_id: ZoneId,
        key: &[u8],
        rf: usize,
        scope: Option<&BTreeSet<NodeId>>,
    ) -> Vec<NodeId> {
        let index = self.range_index(key);
        let mut replicas = self.replicas_of_range(ring, zone_id, index, rf, scope);
        if let Some(pending) = self.pending_of(index) {
            for node in ring.replicas_in_zone(zone_id, pending, rf, scope) {
                if !replicas.contains(&node) {
                    replicas.push(node);
                }
            }
        }
        replicas
    }

    pub fn replicas_of_range(
        &self,
        ring: &Ring,
        zone_id: ZoneId,
        index: usize,
        rf: usize,
        scope: Option<&BTreeSet<NodeId>>,
    ) -> Vec<NodeId> {
        ring.replicas_in_zone(zone_id, self.token_of(index), rf, scope)
    }

    pub fn split(&mut self, at: &[u8]) -> bool {
        if at.is_empty() || self.active_rehomes() > 0 {
            return false;
        }
        let p = &*self.parts;
        let pos = match p.splits.binary_search_by(|s| s.as_slice().cmp(at)) {
            Ok(_) => return false,
            Err(pos) => pos,
        };
        if p.pending[pos].is_some() {
            return false;
        }
        let token = p.tokens[pos].clone();
        let p = self.parts_mut();
        p.splits.insert(pos, at.to_vec());
        p.index = SplitIndexCell::default();
        p.tokens.insert(pos + 1, token);
        p.pending.insert(pos + 1, None);
        p.placement_debt.insert(pos + 1, true);
        true
    }

    pub fn split_canonical(&mut self, at: &[u8]) -> bool {
        if at.is_empty() || self.active_rehomes() > 0 {
            return false;
        }
        let p = &*self.parts;
        let pos = match p.splits.binary_search_by(|s| s.as_slice().cmp(at)) {
            Ok(_) => return false,
            Err(pos) => pos,
        };
        if p.pending[pos].is_some()
            || p.tokens.get(pos + 1).is_some_and(|token| token.as_slice() < at)
        {
            return false;
        }
        let p = self.parts_mut();
        p.splits.insert(pos, at.to_vec());
        p.index = SplitIndexCell::default();
        p.tokens.insert(pos + 1, at.to_vec());
        p.pending.insert(pos + 1, None);
        p.placement_debt.insert(pos + 1, false);
        true
    }

    pub fn split_canonical_with_token(&mut self, at: &[u8], token: Vec<u8>) -> bool {
        if !self.parts.opaque_tokens
            || !valid_opaque_token(&token)
            || at.is_empty()
            || self.active_rehomes() > 0
        {
            return false;
        }
        let p = &*self.parts;
        let pos = match p.splits.binary_search_by(|s| s.as_slice().cmp(at)) {
            Ok(_) => return false,
            Err(pos) => pos,
        };
        if p.pending[pos].is_some() {
            return false;
        }
        let p = self.parts_mut();
        p.splits.insert(pos, at.to_vec());
        p.index = SplitIndexCell::default();
        p.tokens.insert(pos + 1, token);
        p.pending.insert(pos + 1, None);
        p.placement_debt.insert(pos + 1, false);
        true
    }

    pub fn merge_at(&mut self, at: &[u8]) -> bool {
        if self.active_rehomes() > 0 {
            return false;
        }
        let p = &*self.parts;
        let pos = match p.splits.binary_search_by(|s| s.as_slice().cmp(at)) {
            Ok(pos) => pos,
            Err(_) => return false,
        };
        if p.pending[pos].is_some() || p.pending[pos + 1].is_some() {
            return false;
        }
        let p = self.parts_mut();
        let removed = p.splits.remove(pos);
        p.index = SplitIndexCell::default();
        let left_token = p.tokens[pos].clone();
        p.tokens.remove(pos + 1);
        p.pending.remove(pos + 1);
        let right_debt = p.placement_debt.remove(pos + 1);
        if p.opaque_tokens {
            p.placement_debt[pos] |= right_debt;
        }
        for i in pos + 1..p.tokens.len() {
            if p.tokens[i] == removed {
                p.tokens[i] = left_token.clone();
                if p.opaque_tokens {
                    p.placement_debt[i] = true;
                }
            }
        }
        if !p.opaque_tokens {
            p.placement_debt = legacy_placement_debt(&p.splits, &p.tokens);
        }
        true
    }

    pub fn ranges_overlapping(&self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Vec<RangeSlice> {
        if let (Some(l), Some(h)) = (lo, hi) {
            if l >= h {
                return Vec::new();
            }
        }
        let start = match lo {
            Some(l) => self.range_index(l),
            None => 0,
        };
        let end = match hi {
            Some(h) => 1 + self.parts.splits.partition_point(|s| s.as_slice() < h),
            None => self.num_ranges(),
        };
        (start..end)
            .map(|i| {
                let (rlo, rhi) = self.range_bounds(i);
                let slo = max_bound(lo, rlo);
                let shi = min_bound(hi, rhi);
                RangeSlice {
                    index: i,
                    token: self.token_of(i).to_vec(),
                    lo: slo.map(|b| b.to_vec()),
                    hi: shi.map(|b| b.to_vec()),
                }
            })
            .collect()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        if self.parts.opaque_tokens {
            buf.extend_from_slice(&RANGE_MAP_V2_MAGIC.to_le_bytes());
            buf.extend_from_slice(&RANGE_MAP_V4_VERSION.to_le_bytes());
            buf.extend_from_slice(&(self.parts.splits.len() as u32).to_le_bytes());
            put_vecs(&mut buf, &self.parts.splits);
            buf.extend_from_slice(&(self.parts.tokens.len() as u32).to_le_bytes());
            put_vecs(&mut buf, &self.parts.tokens);
            put_pending(&mut buf, &self.parts.pending);
            buf.extend_from_slice(&(self.parts.placement_debt.len() as u32).to_le_bytes());
            for &debt in &self.parts.placement_debt {
                buf.push(u8::from(debt));
            }
        } else if self.active_rehomes() > 0 {
            buf.extend_from_slice(&RANGE_MAP_V2_MAGIC.to_le_bytes());
            buf.extend_from_slice(&RANGE_MAP_V3_VERSION.to_le_bytes());
            buf.extend_from_slice(&(self.parts.splits.len() as u32).to_le_bytes());
            put_vecs(&mut buf, &self.parts.splits);
            buf.extend_from_slice(&(self.parts.tokens.len() as u32).to_le_bytes());
            put_vecs(&mut buf, &self.parts.tokens);
            put_pending(&mut buf, &self.parts.pending);
        } else if self.is_canonical() {
            buf.extend_from_slice(&(self.parts.splits.len() as u32).to_le_bytes());
            put_vecs(&mut buf, &self.parts.splits);
        } else {
            buf.extend_from_slice(&RANGE_MAP_V2_MAGIC.to_le_bytes());
            buf.extend_from_slice(&RANGE_MAP_V2_VERSION.to_le_bytes());
            buf.extend_from_slice(&(self.parts.splits.len() as u32).to_le_bytes());
            put_vecs(&mut buf, &self.parts.splits);
            buf.extend_from_slice(&(self.parts.tokens.len() as u32).to_le_bytes());
            put_vecs(&mut buf, &self.parts.tokens);
        }
        buf
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut p = 0usize;
        let first = take_u32(bytes, &mut p)?;
        let (splits, tokens, pending, placement_debt, opaque_tokens) =
            if first == RANGE_MAP_V2_MAGIC {
                let version = take_u32(bytes, &mut p)?;
                if version != RANGE_MAP_V2_VERSION
                    && version != RANGE_MAP_V3_VERSION
                    && version != RANGE_MAP_V4_VERSION
                {
                    return None;
                }
                let split_count = take_u32(bytes, &mut p)? as usize;
                let splits = take_vecs(bytes, &mut p, split_count)?;
                let token_count = take_u32(bytes, &mut p)? as usize;
                let tokens = take_vecs(bytes, &mut p, token_count)?;
                let pending = if version >= RANGE_MAP_V3_VERSION {
                    take_pending(bytes, &mut p)?
                } else {
                    vec![None; tokens.len()]
                };
                let opaque_tokens = version == RANGE_MAP_V4_VERSION;
                let placement_debt = if opaque_tokens {
                    let count = take_u32(bytes, &mut p)? as usize;
                    let mut debt = Vec::with_capacity(count.min(bytes.len().saturating_sub(p)));
                    for _ in 0..count {
                        match *bytes.get(p)? {
                            0 => debt.push(false),
                            1 => debt.push(true),
                            _ => return None,
                        }
                        p += 1;
                    }
                    debt
                } else {
                    legacy_placement_debt(&splits, &tokens)
                };
                (splits, tokens, pending, placement_debt, opaque_tokens)
            } else {
                let splits = take_vecs(bytes, &mut p, first as usize)?;
                let tokens: Vec<Vec<u8>> = std::iter::once(Vec::new())
                    .chain(splits.iter().cloned())
                    .collect();
                let pending = vec![None; tokens.len()];
                let placement_debt = vec![false; tokens.len()];
                (splits, tokens, pending, placement_debt, false)
            };
        if p != bytes.len() {
            return None;
        }
        RangeMap::from_parts(splits, tokens, pending, placement_debt, opaque_tokens).ok()
    }
}

pub fn data_shard(map: Option<&RangeMap>, key: &[u8], shards: usize) -> usize {
    if shards <= 1 {
        return 0;
    }
    match map {
        Some(map) => (hash64(map.range_token(key)) % shards as u64) as usize,
        None => (hash64(key) % shards as u64) as usize,
    }
}

pub fn scan_shard(map: Option<&RangeMap>, lo: Option<&[u8]>, shards: usize) -> usize {
    if shards <= 1 {
        return 0;
    }
    match map {
        Some(map) => (hash64(map.range_token(lo.unwrap_or(b""))) % shards as u64) as usize,
        None => 0,
    }
}

fn valid_token(splits: &[Vec<u8>], lower: &[u8], token: &[u8], opaque_tokens: bool) -> bool {
    valid_legacy_token(splits, lower, token) || (opaque_tokens && valid_opaque_token(token))
}

fn valid_legacy_token(splits: &[Vec<u8>], lower: &[u8], token: &[u8]) -> bool {
    token <= lower
        && (token.is_empty()
            || splits.binary_search_by(|s| s.as_slice().cmp(token)).is_ok())
}

pub fn valid_opaque_token(token: &[u8]) -> bool {
    token.len() == OPAQUE_TOKEN_LEN
        && token[..4] == OPAQUE_TOKEN_MAGIC
        && token[4] == OPAQUE_TOKEN_VERSION
}

fn legacy_placement_debt(splits: &[Vec<u8>], tokens: &[Vec<u8>]) -> Vec<bool> {
    tokens
        .iter()
        .enumerate()
        .map(|(i, token)| {
            let lower = if i == 0 {
                b"".as_slice()
            } else {
                splits.get(i - 1).map(Vec::as_slice).unwrap_or_default()
            };
            token.as_slice() != lower
        })
        .collect()
}

impl Default for RangeMap {
    fn default() -> Self {
        Self::single()
    }
}

fn put_vecs(buf: &mut Vec<u8>, values: &[Vec<u8>]) {
    for value in values {
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
        buf.extend_from_slice(value);
    }
}

fn put_pending(buf: &mut Vec<u8>, pending: &[Option<Vec<u8>>]) {
    buf.extend_from_slice(&(pending.len() as u32).to_le_bytes());
    for token in pending {
        match token {
            None => buf.push(0),
            Some(token) => {
                buf.push(1);
                buf.extend_from_slice(&(token.len() as u32).to_le_bytes());
                buf.extend_from_slice(token);
            }
        }
    }
}

fn take_pending(bytes: &[u8], p: &mut usize) -> Option<Vec<Option<Vec<u8>>>> {
    let count = take_u32(bytes, p)? as usize;
    let mut pending = Vec::with_capacity(count.min(bytes.len().saturating_sub(*p)));
    for _ in 0..count {
        match *bytes.get(*p)? {
            0 => {
                *p += 1;
                pending.push(None);
            }
            1 => {
                *p += 1;
                pending.push(Some(take_vecs(bytes, p, 1)?.pop()?));
            }
            _ => return None,
        }
    }
    Some(pending)
}

fn take_u32(bytes: &[u8], p: &mut usize) -> Option<u32> {
    let end = p.checked_add(4)?;
    let value = u32::from_le_bytes(bytes.get(*p..end)?.try_into().ok()?);
    *p = end;
    Some(value)
}

fn take_vecs(bytes: &[u8], p: &mut usize, count: usize) -> Option<Vec<Vec<u8>>> {
    let mut values = Vec::with_capacity(count.min(bytes.len().saturating_sub(*p) / 4));
    for _ in 0..count {
        let len = take_u32(bytes, p)? as usize;
        let end = p.checked_add(len)?;
        values.push(bytes.get(*p..end)?.to_vec());
        *p = end;
    }
    Some(values)
}

pub fn even_boundaries(k: u32) -> Vec<Vec<u8>> {
    if k <= 1 {
        return Vec::new();
    }
    let k = k.min(256) as u64;
    let mut out: Vec<Vec<u8>> = Vec::new();
    for i in 1..k {
        let b = (i * 256 / k) as u8;
        if b == 0 {
            continue;
        }
        let boundary = vec![b];
        if out.last() != Some(&boundary) {
            out.push(boundary);
        }
    }
    out
}

fn max_bound<'a>(a: Option<&'a [u8]>, b: Option<&'a [u8]>) -> Option<&'a [u8]> {
    match (a, b) {
        (None, x) | (x, None) => x,
        (Some(a), Some(b)) => Some(if a >= b { a } else { b }),
    }
}

fn min_bound<'a>(a: Option<&'a [u8]>, b: Option<&'a [u8]>) -> Option<&'a [u8]> {
    match (a, b) {
        (None, x) | (x, None) => x,
        (Some(a), Some(b)) => Some(if a <= b { a } else { b }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[test]
    fn single_range_covers_everything() {
        let m = RangeMap::single();
        assert_eq!(m.num_ranges(), 1);
        assert_eq!(m.range_index(b""), 0);
        assert_eq!(m.range_index(b"zzz"), 0);
        assert_eq!(m.range_token(b"anything"), b"");
        assert_eq!(m.range_bounds(0), (None, None));
    }

    #[test]
    fn from_splits_normalizes() {
        let m = RangeMap::from_splits([k("m"), k("d"), k("m"), Vec::new(), k("t")]);
        assert_eq!(m.splits(), &[k("d"), k("m"), k("t")]);
        assert_eq!(m.num_ranges(), 4);
    }

    #[test]
    fn range_index_and_token() {
        let m = RangeMap::from_splits([k("d"), k("m"), k("t")]);
        assert_eq!(m.range_index(b"a"), 0);
        assert_eq!(m.range_index(b"d"), 1);
        assert_eq!(m.range_index(b"ff"), 1);
        assert_eq!(m.range_index(b"m"), 2);
        assert_eq!(m.range_index(b"x"), 3);

        assert_eq!(m.range_token(b"a"), b"");
        assert_eq!(m.range_token(b"d"), b"d");
        assert_eq!(m.range_token(b"ff"), b"d");
        assert_eq!(m.range_token(b"x"), b"t");
    }

    #[test]
    fn indexed_range_index_matches_a_plain_search() {
        fn next(state: &mut u64, n: u64) -> u64 {
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            *state % n
        }
        fn random_key(state: &mut u64, shared: &[u8]) -> Vec<u8> {
            let alphabet = [0u8, 1, b'a', b'b', b'z', 0xff];
            let mut key = shared[..next(state, shared.len() as u64 + 1) as usize].to_vec();
            for _ in 0..next(state, 12) {
                key.push(alphabet[next(state, alphabet.len() as u64) as usize]);
            }
            key
        }
        let state = &mut 0x9e37_79b9_7f4a_7c15u64;
        let shared_tail = RangeMap::from_splits((0..40).map(|i| format!("abc{i:02}").into_bytes()));
        for key in [&b"abd"[..], b"abc", b"abc39", b"abc4", b"b", b"ab", b"abc15\0", b""] {
            let want = shared_tail.splits().partition_point(|s| s.as_slice() <= key);
            assert_eq!(shared_tail.range_index(key), want, "key {key:?}");
        }
        let prefixes: [&[u8]; 3] = [b"", b"app.bsky.feed.like/did:plc:", b"at://did:plc:aaaaaaaa"];
        for (n, shared) in [17usize, 31, 256, 257, 300, 4097, 5000].into_iter().zip(prefixes.iter().cycle()) {
            let mut map = RangeMap::from_splits((0..n * 2).map(|_| random_key(state, shared)));
            let mut probes: Vec<Vec<u8>> = (0..4000).map(|_| random_key(state, shared)).collect();
            for split in map.splits().iter().step_by(7) {
                let mut longer = split.clone();
                longer.push(0);
                let mut shorter = split.clone();
                shorter.pop();
                probes.extend([split.clone(), longer, shorter]);
                for cut in [0, split.len() / 2, split.len() - 1] {
                    if split[cut] < 0xff {
                        let mut bumped = split[..=cut].to_vec();
                        bumped[cut] += 1;
                        probes.push(bumped);
                    }
                }
            }
            probes.push(vec![0xff; 40]);
            for round in 0..3 {
                for key in &probes {
                    let want = map.splits().partition_point(|s| s.as_slice() <= key.as_slice());
                    assert_eq!(map.range_index(key), want, "n {n} round {round} key {key:?}");
                }
                let at = random_key(state, shared);
                if round == 0 {
                    map.split(&at);
                } else {
                    let split = map.splits()[next(state, map.splits().len() as u64) as usize].clone();
                    assert!(map.merge_at(&split));
                }
                probes.push(at);
            }
        }
    }

    #[test]
    fn mutating_a_clone_leaves_the_shared_original_intact() {
        let original = RangeMap::from_splits((0..64).map(|i| format!("k{i:03}").into_bytes()));
        let snapshot = RangeMap::decode(&original.encode()).unwrap();
        assert_eq!(original.range_index(b"k031x"), 32);
        let mut copy = original.clone();
        assert_eq!(copy, original);
        assert!(copy.split(b"k031x"));
        assert!(copy.merge_at(b"k010"));
        assert!(copy.set_pending(5, Vec::new()));
        assert_ne!(copy, original);
        assert_eq!(original, snapshot);
        assert_eq!(original.range_index(b"k031x"), 32);
        assert_eq!(copy.range_index(b"k031x"), 32);
        assert_eq!(copy.range_index(b"k031y"), 32);
        assert_eq!(original.range_index(b"k031y"), 32);
        assert_eq!(original.range_index(b"k010"), 11);
        assert_eq!(copy.range_index(b"k010"), 10);
        assert!(original.pending_of(5).is_none());
        assert!(copy.commit_pending(5));
        assert_eq!(original, snapshot);
        let mut unchanged = original.clone();
        assert!(!unchanged.split(b"k000"));
        assert!(!unchanged.clear_pending(3));
        assert!(Arc::ptr_eq(&unchanged.parts, &original.parts));
    }

    #[test]
    fn range_bounds_are_contiguous() {
        let m = RangeMap::from_splits([k("d"), k("m")]);
        assert_eq!(m.range_bounds(0), (None, Some(&b"d"[..])));
        assert_eq!(m.range_bounds(1), (Some(&b"d"[..]), Some(&b"m"[..])));
        assert_eq!(m.range_bounds(2), (Some(&b"m"[..]), None));
    }

    #[test]
    fn split_is_minimal_and_idempotent() {
        let mut m = RangeMap::from_splits([k("m")]);
        assert!(m.split(b"t"));
        assert_eq!(m.splits(), &[k("m"), k("t")]);
        assert!(!m.split(b"t"));
        assert!(!m.split(b""));
        assert_eq!(m.range_token(b"p"), b"m");
        assert_eq!(m.range_token(b"z"), b"m");
        assert_eq!(m.tokens(), &[Vec::new(), k("m"), k("m")]);
    }

    #[test]
    fn canonical_split_uses_the_new_lower_bound() {
        let mut m = RangeMap::from_splits([k("m")]);
        assert!(m.split_canonical(b"t"));
        assert_eq!(m.splits(), &[k("m"), k("t")]);
        assert_eq!(m.tokens(), &[Vec::new(), k("m"), k("t")]);
        assert!(m.is_canonical());
        assert_eq!(m.unrehomed_ranges(), 0);
        assert!(!m.split_canonical(b"t"));
        assert!(!m.split_canonical(b""));

        let bytes = m.encode();
        assert_eq!(&bytes[..4], &2u32.to_le_bytes());
        assert_eq!(RangeMap::decode(&bytes), Some(m));
    }

    #[test]
    fn canonical_split_refuses_decreasing_tokens_without_mutation() {
        let mut m = RangeMap::from_splits_and_tokens(
            vec![k("m"), k("t")],
            vec![Vec::new(), Vec::new(), k("m")],
        )
        .unwrap();
        let before = m.clone();
        assert!(!m.split_canonical(b"p"));
        assert_eq!(m, before);
    }

    #[test]
    fn opaque_canonical_split_installs_debt_free_token() {
        let left = opaque_placement_token([1; OPAQUE_TOKEN_SEED_LEN]);
        let right = opaque_placement_token([2; OPAQUE_TOKEN_SEED_LEN]);
        let mut m = RangeMap::from_opaque_parts(
            Vec::new(),
            vec![left],
            vec![None],
            vec![false],
        )
        .unwrap();
        assert!(m.split_canonical_with_token(b"m", right.clone()));
        assert_eq!(m.tokens(), &[opaque_placement_token([1; OPAQUE_TOKEN_SEED_LEN]), right]);
        assert_eq!(m.placement_debt(), &[false, false]);
        assert_eq!(RangeMap::decode(&m.encode()), Some(m));
    }

    #[test]
    fn merge_removes_a_boundary() {
        let mut m = RangeMap::from_splits([k("d"), k("m")]);
        assert!(m.merge_at(b"d"));
        assert_eq!(m.splits(), &[k("m")]);
        assert!(!m.merge_at(b"d"));
    }

    #[test]
    fn overlapping_full_scan_covers_all_ranges_clipped() {
        let m = RangeMap::from_splits([k("d"), k("m")]);
        let slices = m.ranges_overlapping(None, None);
        assert_eq!(slices.len(), 3);
        assert_eq!(slices[0].lo, None);
        assert_eq!(slices[0].hi, Some(k("d")));
        assert_eq!(slices[1].lo, Some(k("d")));
        assert_eq!(slices[1].hi, Some(k("m")));
        assert_eq!(slices[2].lo, Some(k("m")));
        assert_eq!(slices[2].hi, None);
        assert_eq!(slices[0].token, b"");
        assert_eq!(slices[1].token, k("d"));
        assert_eq!(slices[2].token, k("m"));
    }

    #[test]
    fn overlapping_bounded_query_clips_endpoints() {
        let m = RangeMap::from_splits([k("d"), k("m"), k("t")]);
        let slices = m.ranges_overlapping(Some(b"f"), Some(b"p"));
        assert_eq!(slices.len(), 2);
        assert_eq!(slices[0].index, 1);
        assert_eq!(slices[0].lo, Some(k("f")));
        assert_eq!(slices[0].hi, Some(k("m")));
        assert_eq!(slices[1].index, 2);
        assert_eq!(slices[1].lo, Some(k("m")));
        assert_eq!(slices[1].hi, Some(k("p")));
    }

    #[test]
    fn overlapping_query_inside_one_range() {
        let m = RangeMap::from_splits([k("d"), k("m"), k("t")]);
        let slices = m.ranges_overlapping(Some(b"e"), Some(b"g"));
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].index, 1);
        assert_eq!(slices[0].lo, Some(k("e")));
        assert_eq!(slices[0].hi, Some(k("g")));
    }

    #[test]
    fn overlapping_query_on_a_boundary_excludes_the_empty_upper() {
        let m = RangeMap::from_splits([k("d"), k("m")]);
        let slices = m.ranges_overlapping(Some(b"a"), Some(b"d"));
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].index, 0);
        assert_eq!(slices[0].hi, Some(k("d")));
    }

    #[test]
    fn empty_query_yields_nothing() {
        let m = RangeMap::from_splits([k("d")]);
        assert!(m.ranges_overlapping(Some(b"x"), Some(b"x")).is_empty());
        assert!(m.ranges_overlapping(Some(b"y"), Some(b"x")).is_empty());
    }

    #[test]
    fn lower_bounded_only_scan() {
        let m = RangeMap::from_splits([k("d"), k("m")]);
        let slices = m.ranges_overlapping(Some(b"g"), None);
        assert_eq!(slices.len(), 2);
        assert_eq!(slices[0].index, 1);
        assert_eq!(slices[0].lo, Some(k("g")));
        assert_eq!(slices[1].index, 2);
        assert_eq!(slices[1].hi, None);
    }

    #[test]
    fn split_preserves_placement_for_every_key() {
        let ring = Ring::from_members(3, 64, [1, 2, 3, 4, 5]);
        let zone = 7;
        let before = RangeMap::from_splits([k("m")]);
        let mut after = before.clone();
        after.split(b"t");

        for key in ["", "a", "m", "ma", "p", "s", "t", "u", "z"] {
            let kb = key.as_bytes();
            assert_eq!(
                before.replicas_for_key(&ring, zone, kb, 3, None),
                after.replicas_for_key(&ring, zone, kb, 3, None),
                "key {key} must not move"
            );
        }
        assert!(!after.is_canonical());
        assert_eq!(after.unrehomed_ranges(), 1);
    }

    #[test]
    fn pending_placement_is_the_union_of_current_and_target() {
        let ring = Ring::from_members(2, 64, [1, 2, 3, 4]);
        let mut map = RangeMap::single();
        assert!(map.split(b"m"));
        assert!(map.set_pending(1, b"m".to_vec()));
        let mut want = map.replicas_of_range(&ring, 7, 1, 2, None);
        for node in ring.replicas_in_zone(7, b"m", 2, None) {
            if !want.contains(&node) {
                want.push(node);
            }
        }
        assert_eq!(map.replicas_for_key_while_rehoming(&ring, 7, b"z", 2, None), want);
        assert_eq!(
            map.replicas_for_key_while_rehoming(&ring, 7, b"a", 2, None),
            map.replicas_for_key(&ring, 7, b"a", 2, None)
        );
    }

    #[test]
    fn keys_in_a_range_co_locate() {
        let ring = Ring::from_members(3, 128, [1, 2, 3, 4, 5]);
        let m = RangeMap::from_splits([k("d"), k("m"), k("t")]);
        let zone = 3;
        let want = m.replicas_of_range(&ring, zone, 1, 3, None);
        for key in ["d", "de", "ff", "lz"] {
            assert_eq!(
                m.replicas_for_key(&ring, zone, key.as_bytes(), 3, None),
                want,
                "key {key} must co-locate with its range"
            );
        }
    }

    #[test]
    fn encode_decode_roundtrip() {
        let m = RangeMap::from_splits([k("apple"), k("mango"), k("zebra")]);
        let bytes = m.encode();
        assert_eq!(&bytes[..4], &3u32.to_le_bytes());
        assert_eq!(RangeMap::decode(&bytes), Some(m));

        let mut inherited = RangeMap::from_splits([k("m")]);
        inherited.split(b"t");
        let bytes = inherited.encode();
        assert_eq!(&bytes[..4], &RANGE_MAP_V2_MAGIC.to_le_bytes());
        assert_eq!(RangeMap::decode(&bytes), Some(inherited));

        let s = RangeMap::single();
        assert_eq!(RangeMap::decode(&s.encode()), Some(s));
        assert_eq!(RangeMap::decode(&[5, 0, 0, 0]), None);
    }

    #[test]
    fn validates_explicit_tokens() {
        let splits = vec![k("d"), k("m"), k("t")];
        assert!(RangeMap::from_splits_and_tokens(
            splits.clone(),
            vec![Vec::new(), Vec::new(), k("d"), k("d")],
        )
        .is_ok());
        assert!(RangeMap::from_splits_and_tokens(splits.clone(), vec![Vec::new()]).is_err());
        assert!(RangeMap::from_splits_and_tokens(
            splits.clone(),
            vec![Vec::new(), k("d"), k("x"), k("x")],
        )
        .is_err());
        assert!(RangeMap::from_splits_and_tokens(
            splits.clone(),
            vec![Vec::new(), k("d"), Vec::new(), k("t")],
        )
        .is_ok());
        assert!(RangeMap::from_splits_and_tokens(
            vec![k("m"), k("d")],
            vec![Vec::new(), k("m"), k("m")],
        )
        .is_err());
    }

    #[test]
    fn pending_roundtrip_commit_and_guards() {
        let mut map = RangeMap::from_splits([k("m")]);
        assert!(map.split(b"t"));
        assert!(map.set_pending(2, k("t")));
        assert_eq!(map.pending_for_key(b"z"), Some(b"t".as_slice()));
        assert_eq!(map.active_rehomes(), 1);
        assert!(!map.split(b"x"));
        assert!(!map.merge_at(b"t"));
        let bytes = map.encode();
        assert_eq!(&bytes[4..8], &RANGE_MAP_V3_VERSION.to_le_bytes());
        assert_eq!(RangeMap::decode(&bytes), Some(map.clone()));
        assert!(map.commit_pending(2));
        assert_eq!(map.token_of(2), b"t");
        assert_eq!(map.active_rehomes(), 0);
        assert_eq!(&map.encode()[..4], &2u32.to_le_bytes());
    }

    #[test]
    fn legacy_prior_boundary_commit_stays_v2_and_roundtrips_routing() {
        let ring = Ring::from_members(2, 64, [1, 2, 3, 4]);
        let mut map = RangeMap::from_splits([k("d"), k("m"), k("t")]);
        assert!(map.set_pending(3, k("d")));
        assert!(map.commit_pending(3));
        assert_eq!(map.token_of(3), b"d");
        assert!(map.placement_debt_of(3));
        let replicas = map.replicas_for_key(&ring, 4, b"z", 2, None);
        let shard = data_shard(Some(&map), b"z", 8);
        let bytes = map.encode();
        assert_eq!(&bytes[..4], &RANGE_MAP_V2_MAGIC.to_le_bytes());
        assert_eq!(&bytes[4..8], &RANGE_MAP_V2_VERSION.to_le_bytes());
        let decoded = RangeMap::decode(&bytes).unwrap();
        assert_eq!(decoded.token_of(3), b"d");
        assert_eq!(decoded.replicas_for_key(&ring, 4, b"z", 2, None), replicas);
        assert_eq!(data_shard(Some(&decoded), b"z", 8), shard);
        assert_eq!(decoded, map);
    }

    #[test]
    fn nonadjacent_pending_refuses_all_structural_changes() {
        let mut map = RangeMap::from_splits([k("d"), k("m"), k("t")]);
        assert!(map.set_pending(3, k("d")));
        for op in [0, 1, 2] {
            let mut attempted = map.clone();
            let changed = match op {
                0 => attempted.split(b"f"),
                1 => attempted.split_canonical(b"g"),
                _ => attempted.merge_at(b"d"),
            };
            assert!(!changed);
            assert_eq!(attempted, map);
            assert_eq!(RangeMap::decode(&attempted.encode()), Some(attempted));
        }
    }

    #[test]
    fn middle_rehome_commit_v2_roundtrips_with_nonmonotonic_tokens() {
        let mut map = RangeMap::single();
        assert!(map.split(b"m"));
        assert!(map.split(b"t"));
        assert_eq!(map.tokens(), &[Vec::new(), Vec::new(), Vec::new()]);
        assert!(map.set_pending(1, b"m".to_vec()));
        assert!(map.commit_pending(1));
        assert_eq!(map.tokens(), &[Vec::new(), b"m".to_vec(), Vec::new()]);
        let bytes = map.encode();
        assert_eq!(&bytes[..4], &RANGE_MAP_V2_MAGIC.to_le_bytes());
        assert_eq!(&bytes[4..8], &RANGE_MAP_V2_VERSION.to_le_bytes());
        assert_eq!(RangeMap::decode(&bytes), Some(map));
    }

    #[test]
    fn pending_validation_rejects_bad_targets() {
        assert!(RangeMap::from_splits_tokens_and_pending(
            vec![k("m"), k("t")],
            vec![Vec::new(), k("m"), k("m")],
            vec![None, None, Some(k("x"))],
        ).is_err());
        assert!(RangeMap::from_splits_tokens_and_pending(
            vec![k("m"), k("t")],
            vec![Vec::new(), k("m"), k("m")],
            vec![None, None],
        ).is_err());
        let mut map = RangeMap::from_splits([k("m")]);
        assert!(!map.set_pending(1, k("z")));
        assert!(!map.set_pending(1, k("m")));
    }

    #[test]
    fn unknown_pending_version_and_trailing_bytes_are_rejected() {
        let mut map = RangeMap::from_splits([k("m")]);
        assert!(map.split(b"t"));
        assert!(map.set_pending(2, k("t")));
        let mut bytes = map.encode();
        bytes[4..8].copy_from_slice(&99u32.to_le_bytes());
        assert_eq!(RangeMap::decode(&bytes), None);
        let mut bytes = map.encode();
        bytes.push(0);
        assert_eq!(RangeMap::decode(&bytes), None);
    }

    #[test]
    fn opaque_v4_roundtrip_and_legacy_fail_closed() {
        let opaque = opaque_placement_token([7; OPAQUE_TOKEN_SEED_LEN]);
        assert!(RangeMap::from_splits_tokens_and_pending(
            vec![k("m")], vec![Vec::new(), opaque.clone()], vec![None, None]
        ).is_err());

        let map = RangeMap::from_opaque_parts(
            vec![k("m")],
            vec![Vec::new(), opaque.clone()],
            vec![None, Some(opaque_placement_token([9; OPAQUE_TOKEN_SEED_LEN]))],
            vec![false, true],
        ).unwrap();
        let bytes = map.encode();
        assert_eq!(&bytes[..4], &RANGE_MAP_V2_MAGIC.to_le_bytes());
        assert_eq!(&bytes[4..8], &RANGE_MAP_V4_VERSION.to_le_bytes());
        assert_eq!(RangeMap::decode(&bytes), Some(map));

        let mut malformed = opaque;
        malformed[4] = OPAQUE_TOKEN_VERSION + 1;
        assert!(RangeMap::from_opaque_parts(
            vec![k("m")], vec![Vec::new(), malformed], vec![None, None], vec![false, false]
        ).is_err());
        assert!(RangeMap::from_opaque_parts(
            vec![k("m")], vec![Vec::new(), k("m")], vec![None, None], vec![false]
        ).is_err());
    }

    #[test]
    fn opaque_debt_transitions_do_not_affect_routing() {
        let ring = Ring::from_members(2, 64, [1, 2, 3, 4]);
        let mut map = RangeMap::from_splits([k("m")]);
        map.enable_opaque_tokens();
        assert!(map.set_opaque_pending(1, [3; OPAQUE_TOKEN_SEED_LEN]));
        let pending = map.pending_of(1).unwrap().to_vec();
        assert_eq!(map.unrehomed_ranges(), 0);
        assert!(map.clear_pending(1));
        assert_eq!(map.unrehomed_ranges(), 0);
        assert!(map.set_pending(1, pending.clone()));
        assert!(map.commit_pending(1));
        assert_eq!(map.token_of(1), pending);
        assert_eq!(map.unrehomed_ranges(), 0);

        let replicas = map.replicas_for_key(&ring, 9, b"z", 2, None);
        let shard = data_shard(Some(&map), b"z", 8);
        assert!(map.split(b"t"));
        assert_eq!(map.placement_debt(), &[false, false, true]);
        assert_eq!(map.replicas_for_key(&ring, 9, b"z", 2, None), replicas);
        assert_eq!(data_shard(Some(&map), b"z", 8), shard);
        assert!(map.merge_at(b"t"));
        assert_eq!(map.placement_debt(), &[false, true]);
    }

    #[test]
    fn opaque_rehome_refuses_split_and_merge_then_commit_clears_debt() {
        let mut map = RangeMap::from_splits([k("m")]);
        assert!(map.split(b"t"));
        map.enable_opaque_tokens();
        assert!(map.set_opaque_pending(2, [11; OPAQUE_TOKEN_SEED_LEN]));
        let before = map.clone();
        assert!(!map.split(b"x"));
        assert!(!map.merge_at(b"t"));
        assert_eq!(map, before);
        assert!(map.commit_pending(2));
        assert_eq!(map.unrehomed_ranges(), 0);
        assert!(valid_opaque_token(map.token_of(2)));
    }

    #[test]
    fn shard_routing_matches_algorithms_doc() {
        let m = RangeMap::from_splits([k("f"), k("m"), k("s")]);
        for shards in [2usize, 4, 5] {
            for key in ["", "a", "f", "fzz", "m", "q", "s", "zz"] {
                let kb = key.as_bytes();
                assert_eq!(
                    data_shard(Some(&m), kb, shards),
                    (hash64(m.range_token(kb)) % shards as u64) as usize
                );
                assert_eq!(
                    data_shard(None, kb, shards),
                    (hash64(kb) % shards as u64) as usize
                );
                assert_eq!(scan_shard(Some(&m), Some(kb), shards), data_shard(Some(&m), kb, shards));
                assert_eq!(scan_shard(None, Some(kb), shards), 0);
            }
            assert_eq!(scan_shard(Some(&m), None, shards), data_shard(Some(&m), b"", shards));
        }
        assert_eq!(data_shard(Some(&m), b"q", 1), 0);
        assert_eq!(data_shard(None, b"q", 0), 0);
        assert_eq!(scan_shard(Some(&m), Some(b"q"), 1), 0);
    }

    #[test]
    fn even_boundaries_spread() {
        assert!(even_boundaries(0).is_empty());
        assert!(even_boundaries(1).is_empty());
        for k in 2..=256u32 {
            let bs = even_boundaries(k);
            assert_eq!(bs.len() as u32, k - 1, "k={k} boundary count");
            for w in bs.windows(2) {
                assert!(w[0] < w[1], "k={k} strictly ascending");
            }
            for b in &bs {
                assert!(!b.is_empty() && b != &vec![0u8], "k={k} valid boundary");
            }
            let m = RangeMap::from_splits(bs);
            assert_eq!(m.num_ranges() as u32, k, "k={k} range count");
        }
        assert_eq!(even_boundaries(1000).len(), 255);
    }
}
