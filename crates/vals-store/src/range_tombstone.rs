use crate::crc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtRecord {
    pub lo: Option<Vec<u8>>,
    pub hi: Option<Vec<u8>>,
    pub version: u128,
    pub seq: u64,
}

impl RtRecord {
    pub fn span_is_empty(&self) -> bool {
        matches!((&self.lo, &self.hi), (Some(lo), Some(hi)) if lo >= hi)
    }

    fn encoded_len(&self) -> usize {
        RT_RECORD_FIXED_LEN + self.lo.as_ref().map_or(0, Vec::len) + self.hi.as_ref().map_or(0, Vec::len)
    }

    pub fn without(&self, spans: &FragmentSet) -> Vec<RtRecord> {
        if spans.is_empty() {
            return vec![self.clone()];
        }
        let piece = |lo: &[u8], hi: Option<&[u8]>| RtRecord {
            lo: if lo.is_empty() { self.lo.clone() } else { Some(lo.to_vec()) },
            hi: hi.map(<[u8]>::to_vec),
            version: self.version,
            seq: self.seq,
        };
        let mut out = Vec::new();
        let mut at = self.lo.clone().unwrap_or_default();
        for span in spans.clipped(&at, self.hi.as_deref()).into_iter().filter(|span| span.version >= self.version) {
            if span.lo > at {
                out.push(piece(&at, Some(&span.lo)));
            }
            match span.hi {
                Some(hi) => at = hi,
                None => return out,
            }
        }
        if self.hi.as_deref().is_none_or(|hi| at.as_slice() < hi) {
            out.push(piece(&at, self.hi.as_deref()));
        }
        out
    }
}

fn hi_at_least(a: Option<&[u8]>, b: Option<&[u8]>) -> bool {
    match (a, b) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(a), Some(b)) => a >= b,
    }
}

type MaximalSpans<'a> = std::collections::BTreeMap<&'a [u8], Option<&'a [u8]>>;

fn span_contained(maximal: &MaximalSpans<'_>, lo: &[u8], hi: Option<&[u8]>) -> bool {
    use std::ops::Bound;
    maximal
        .range::<[u8], _>((Bound::Unbounded, Bound::Included(lo)))
        .next_back()
        .is_some_and(|(_, h)| hi_at_least(*h, hi))
}

pub(crate) fn drop_dominated(records: &mut Vec<RtRecord>, through: u64) -> usize {
    use std::ops::Bound;
    let mut order: Vec<usize> = (0..records.len()).filter(|&i| records[i].seq <= through).collect();
    if order.len() < 2 {
        return 0;
    }
    order.sort_unstable_by(|&a, &b| records[b].version.cmp(&records[a].version));
    let mut dominated = vec![false; records.len()];
    let mut maximal = MaximalSpans::new();
    for group in order.chunk_by(|&a, &b| records[a].version == records[b].version) {
        for &i in group {
            dominated[i] = span_contained(&maximal, records[i].lo.as_deref().unwrap_or(&[]), records[i].hi.as_deref());
        }
        for &i in group.iter().filter(|&&i| !dominated[i]) {
            let (lo, hi) = (records[i].lo.as_deref().unwrap_or(&[]), records[i].hi.as_deref());
            if span_contained(&maximal, lo, hi) {
                continue;
            }
            let inside: Vec<&[u8]> = maximal
                .range::<[u8], _>((Bound::Included(lo), Bound::Unbounded))
                .take_while(|(_, h)| hi_at_least(hi, **h))
                .map(|(l, _)| *l)
                .collect();
            for l in inside {
                maximal.remove(l);
            }
            maximal.insert(lo, hi);
        }
    }
    drop(maximal);
    let n = dominated.iter().filter(|d| **d).count();
    if n > 0 {
        let mut at = 0;
        records.retain(|_| {
            at += 1;
            !dominated[at - 1]
        });
    }
    n
}

#[inline]
pub fn version_hlc(version: u128) -> u64 {
    (version >> 64) as u64
}

const LO_PRESENT: u8 = 0x01;
const HI_PRESENT: u8 = 0x02;
const RT_RECORD_FIXED_LEN: usize = 1 + 4 + 4 + 16 + 8;

pub(crate) fn rt_block_len(records: &[RtRecord]) -> usize {
    rt_block_len_of(records.iter())
}

pub(crate) fn rt_block_len_of<'a>(records: impl Iterator<Item = &'a RtRecord>) -> usize {
    let (n, bytes) = records.fold((0usize, 0usize), |(n, b), r| (n + 1, b + r.encoded_len()));
    if n == 0 { 0 } else { 4 + bytes + 4 }
}

fn encode_bounds(b: &mut Vec<u8>, lo: Option<&[u8]>, hi: Option<&[u8]>) {
    let flags = if lo.is_some() { LO_PRESENT } else { 0 } | if hi.is_some() { HI_PRESENT } else { 0 };
    b.push(flags);
    for bound in [lo, hi] {
        let bytes = bound.unwrap_or(&[]);
        b.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        b.extend_from_slice(bytes);
    }
}

type Bounds = (Option<Vec<u8>>, Option<Vec<u8>>);

struct Reader<'a> {
    b: &'a [u8],
    off: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], &'static str> {
        let end = self.off.checked_add(n).ok_or("range tombstone record truncated")?;
        let s = self.b.get(self.off..end).ok_or("range tombstone record truncated")?;
        self.off = end;
        Ok(s)
    }

    fn bounds(&mut self) -> Result<Bounds, &'static str> {
        let flags = self.take(1)?[0];
        if flags & !(LO_PRESENT | HI_PRESENT) != 0 {
            return Err("range tombstone record has unknown flags");
        }
        let mut bounds = [None, None];
        for (i, present) in [LO_PRESENT, HI_PRESENT].into_iter().enumerate() {
            let len = u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as usize;
            let bytes = self.take(len)?;
            if flags & present != 0 {
                bounds[i] = Some(bytes.to_vec());
            } else if len != 0 {
                return Err("range tombstone record has bytes for an absent bound");
            }
        }
        let [lo, hi] = bounds;
        if matches!((&lo, &hi), (Some(l), Some(h)) if l >= h) {
            return Err("range tombstone record has an empty span");
        }
        Ok((lo, hi))
    }
}

pub fn encode_rt_value(lo: Option<&[u8]>, hi: Option<&[u8]>) -> Vec<u8> {
    let mut b = Vec::with_capacity(9 + lo.map_or(0, <[u8]>::len) + hi.map_or(0, <[u8]>::len));
    encode_bounds(&mut b, lo, hi);
    b
}

pub fn decode_rt_value(value: &[u8]) -> Result<Bounds, &'static str> {
    let mut r = Reader { b: value, off: 0 };
    let bounds = r.bounds()?;
    if r.off != value.len() {
        return Err("range tombstone record has trailing bytes");
    }
    Ok(bounds)
}

pub(crate) fn encode_rt_block(records: &[RtRecord]) -> Vec<u8> {
    let mut b = Vec::with_capacity(rt_block_len(records));
    b.extend_from_slice(&(records.len() as u32).to_le_bytes());
    for r in records {
        encode_bounds(&mut b, r.lo.as_deref(), r.hi.as_deref());
        b.extend_from_slice(&r.version.to_le_bytes());
        b.extend_from_slice(&r.seq.to_le_bytes());
    }
    let crc = crc::crc32(&b);
    b.extend_from_slice(&crc.to_le_bytes());
    b
}

pub(crate) fn decode_rt_block(bytes: &[u8]) -> Result<Vec<RtRecord>, &'static str> {
    let Some(body_len) = bytes.len().checked_sub(4) else {
        return Err("range tombstone block truncated");
    };
    let body = &bytes[..body_len];
    if crc::crc32(body) != u32::from_le_bytes(bytes[body_len..].try_into().unwrap()) {
        return Err("range tombstone block crc mismatch");
    }
    let mut r = Reader { b: body, off: 0 };
    let count = u32::from_le_bytes(r.take(4).map_err(|_| "range tombstone block truncated")?.try_into().unwrap()) as usize;
    if count == 0 || count > body_len / RT_RECORD_FIXED_LEN {
        return Err("range tombstone block count out of range");
    }
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let (lo, hi) = r.bounds()?;
        let version = u128::from_le_bytes(r.take(16)?.try_into().unwrap());
        let seq = u64::from_le_bytes(r.take(8)?.try_into().unwrap());
        out.push(RtRecord { lo, hi, version, seq });
    }
    if r.off != body_len {
        return Err("range tombstone block has trailing bytes");
    }
    Ok(out)
}

pub const DEFAULT_MAX_FRAGMENTS: usize = 65_536;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fragment {
    pub lo: Vec<u8>,
    pub hi: Option<Vec<u8>>,
    pub version: u128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverDelta {
    pub lo: Vec<u8>,
    pub hi: Option<Vec<u8>>,
    pub old: Option<u128>,
    pub new: u128,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FragmentCapExceeded {
    pub fragments: usize,
    pub max: usize,
}

impl std::fmt::Display for FragmentCapExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "range tombstone refused: it would grow the fragment set to {} (max {})", self.fragments, self.max)
    }
}

type FragmentMap = std::collections::BTreeMap<Vec<u8>, (Option<Vec<u8>>, u128)>;
type FragmentEntry<'a> = (&'a Vec<u8>, &'a (Option<Vec<u8>>, u128));
type FragmentRange<'a> = std::collections::btree_map::Range<'a, Vec<u8>, (Option<Vec<u8>>, u128)>;

#[derive(Debug, Clone)]
pub struct FragmentSet {
    map: FragmentMap,
    max_fragments: usize,
}

impl Default for FragmentSet {
    fn default() -> Self {
        FragmentSet { map: FragmentMap::new(), max_fragments: DEFAULT_MAX_FRAGMENTS }
    }
}

impl PartialEq for FragmentSet {
    fn eq(&self, other: &Self) -> bool {
        self.map == other.map
    }
}

impl Eq for FragmentSet {}

fn below_hi(k: &[u8], hi: &Option<Vec<u8>>) -> bool {
    hi.as_deref().is_none_or(|h| k < h)
}

type Point = Option<Vec<u8>>;

fn point_lt(a: &Point, b: &Point) -> bool {
    match (a, b) {
        (_, None) => a.is_some(),
        (None, Some(_)) => false,
        (Some(a), Some(b)) => a < b,
    }
}

impl FragmentSet {
    pub fn with_max_fragments(max_fragments: usize) -> Self {
        FragmentSet { max_fragments, ..Self::default() }
    }

    pub fn from_records<'a>(records: impl IntoIterator<Item = &'a RtRecord>) -> Self {
        let mut set = FragmentSet::with_max_fragments(usize::MAX);
        for r in records {
            set.overlay(r.lo.as_deref(), r.hi.as_deref(), r.version).expect("an uncapped overlay never refuses");
        }
        set.max_fragments = DEFAULT_MAX_FRAGMENTS;
        set
    }

    pub fn set_max_fragments(&mut self, max_fragments: usize) {
        self.max_fragments = max_fragments;
    }

    pub fn max_fragments(&self) -> usize {
        self.max_fragments
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn fragments(&self) -> impl Iterator<Item = Fragment> + '_ {
        self.map.iter().map(|(lo, (hi, version))| Fragment { lo: lo.clone(), hi: hi.clone(), version: *version })
    }

    pub fn cover(&self, k: &[u8]) -> Option<u128> {
        use std::ops::Bound;
        self.map
            .range::<[u8], _>((Bound::Unbounded, Bound::Included(k)))
            .next_back()
            .filter(|(_, (hi, _))| below_hi(k, hi))
            .map(|(_, (_, v))| *v)
    }

    pub fn fragment_at(&self, k: &[u8]) -> Option<(&[u8], Option<&[u8]>, u128)> {
        use std::ops::Bound;
        self.map
            .range::<[u8], _>((Bound::Unbounded, Bound::Included(k)))
            .next_back()
            .filter(|(_, (hi, _))| below_hi(k, hi))
            .map(|(lo, (hi, v))| (lo.as_slice(), hi.as_deref(), *v))
    }

    pub fn cover_region(&self, k: &[u8]) -> (Option<u128>, Option<Vec<u8>>) {
        use std::ops::Bound;
        if let Some((_, hi, version)) = self.fragment_at(k) {
            return (Some(version), hi.map(<[u8]>::to_vec));
        }
        let next = self.map.range::<[u8], _>((Bound::Excluded(k), Bound::Unbounded)).next();
        (None, next.map(|(lo, _)| lo.clone()))
    }

    pub fn clipped(&self, lo: &[u8], hi: Option<&[u8]>) -> Vec<Fragment> {
        use std::ops::Bound;
        if hi.is_some_and(|h| h <= lo) {
            return Vec::new();
        }
        let first = self.fragment_at(lo).map(|(flo, _, _)| flo).unwrap_or(lo);
        self.map
            .range::<[u8], _>((Bound::Included(first), Bound::Unbounded))
            .take_while(|(flo, _)| hi.is_none_or(|h| flo.as_slice() < h))
            .map(|(flo, (fhi, version))| Fragment {
                lo: flo.as_slice().max(lo).to_vec(),
                hi: match (fhi.as_deref(), hi) {
                    (Some(a), Some(b)) => Some(a.min(b).to_vec()),
                    (a, b) => a.or(b).map(<[u8]>::to_vec),
                },
                version: *version,
            })
            .collect()
    }

    pub fn covers_span(&self, lo: &[u8], hi: Option<&[u8]>, version: u128) -> bool {
        let mut at = lo.to_vec();
        for f in self.clipped(lo, hi) {
            if f.lo != at || f.version < version {
                return false;
            }
            match f.hi {
                Some(next) => at = next,
                None => return true,
            }
        }
        hi.is_some_and(|hi| at.as_slice() >= hi)
    }

    pub fn cursor_from(&self, start: &[u8]) -> CoverCursor<'_> {
        use std::ops::Bound;
        CoverCursor {
            cur: self.map.range::<[u8], _>((Bound::Unbounded, Bound::Included(start))).next_back(),
            rest: self.map.range::<[u8], _>((Bound::Excluded(start), Bound::Unbounded)).peekable(),
            #[cfg(debug_assertions)]
            last: start.to_vec(),
        }
    }

    pub fn overlay(&mut self, lo: Option<&[u8]>, hi: Option<&[u8]>, version: u128) -> Result<Vec<CoverDelta>, FragmentCapExceeded> {
        Ok(match self.plan_overlay(lo, hi, version)? {
            Some(plan) => self.apply_overlay(plan),
            None => Vec::new(),
        })
    }

    pub fn apply_overlay(&mut self, plan: OverlayPlan) -> Vec<CoverDelta> {
        for lo in &plan.remove {
            self.map.remove(lo);
        }
        for (plo, phi, v) in plan.insert {
            self.map.insert(plo, (phi, v));
        }
        plan.delta
    }

    pub fn plan_overlay(&self, lo: Option<&[u8]>, hi: Option<&[u8]>, version: u128) -> Result<Option<OverlayPlan>, FragmentCapExceeded> {
        use std::ops::Bound;
        let lo: &[u8] = lo.unwrap_or(&[]);
        if hi.is_some_and(|h| h <= lo) {
            return Ok(None);
        }
        let hi_point: Point = hi.map(<[u8]>::to_vec);
        let start: Vec<u8> = match self.map.range::<[u8], _>((Bound::Unbounded, Bound::Excluded(lo))).next_back() {
            Some((flo, (fhi, _))) if fhi.as_deref().is_none_or(|h| h >= lo) => flo.clone(),
            _ => lo.to_vec(),
        };
        let window: Vec<(Vec<u8>, Point, u128)> = self
            .map
            .range::<[u8], _>((Bound::Included(start.as_slice()), Bound::Unbounded))
            .take_while(|(flo, _)| hi.is_none_or(|h| flo.as_slice() <= h))
            .map(|(flo, (fhi, v))| (flo.clone(), fhi.clone(), *v))
            .collect();

        let mut points: Vec<Point> = Vec::with_capacity(2 * window.len() + 2);
        points.push(Some(lo.to_vec()));
        points.push(hi_point.clone());
        for (flo, fhi, _) in &window {
            points.push(Some(flo.clone()));
            points.push(fhi.clone());
        }
        points.sort_by(|a, b| match (a, b) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (Some(_), None) => std::cmp::Ordering::Less,
            (Some(a), Some(b)) => a.cmp(b),
        });
        points.dedup();

        let mut pieces: Vec<(Vec<u8>, Point, u128)> = Vec::new();
        let mut delta = Vec::new();
        let mut w = 0usize;
        for pair in points.windows(2) {
            let (Some(p), q) = (&pair[0], &pair[1]) else { continue };
            while w < window.len() && !point_lt(&Some(p.clone()), &window[w].1) {
                w += 1;
            }
            let old = window.get(w).filter(|(flo, _, _)| flo.as_slice() <= p.as_slice()).map(|(_, _, v)| *v);
            let inside = p.as_slice() >= lo && below_hi(p, &hi_point);
            let new = if inside { Some(old.map_or(version, |o| o.max(version))) } else { old };
            if inside && old.is_none_or(|o| o < version) {
                delta.push(CoverDelta { lo: p.clone(), hi: q.clone(), old, new: version });
            }
            let Some(new) = new else { continue };
            match pieces.last_mut() {
                Some((_, last_hi, last_v)) if *last_v == new && last_hi.as_deref() == Some(p.as_slice()) => {
                    *last_hi = q.clone();
                }
                _ => pieces.push((p.clone(), q.clone(), new)),
            }
        }
        if delta.is_empty() {
            return Ok(None);
        }
        let fragments = self.map.len() - window.len() + pieces.len();
        if fragments > self.max_fragments && fragments > self.map.len() {
            return Err(FragmentCapExceeded { fragments, max: self.max_fragments });
        }
        Ok(Some(OverlayPlan {
            remove: window.into_iter().map(|(flo, _, _)| flo).collect(),
            insert: pieces,
            delta,
        }))
    }
}

pub struct OverlayPlan {
    remove: Vec<Vec<u8>>,
    insert: Vec<(Vec<u8>, Point, u128)>,
    delta: Vec<CoverDelta>,
}

impl OverlayPlan {
    pub fn delta(&self) -> &[CoverDelta] {
        &self.delta
    }
}

pub struct CoverCursor<'a> {
    cur: Option<FragmentEntry<'a>>,
    rest: std::iter::Peekable<FragmentRange<'a>>,
    #[cfg(debug_assertions)]
    last: Vec<u8>,
}

impl CoverCursor<'_> {
    pub fn cover(&mut self, k: &[u8]) -> Option<u128> {
        #[cfg(debug_assertions)]
        {
            debug_assert!(k >= self.last.as_slice(), "cover cursor keys must not decrease");
            self.last.clear();
            self.last.extend_from_slice(k);
        }
        while self.rest.peek().is_some_and(|(lo, _)| lo.as_slice() <= k) {
            self.cur = self.rest.next();
        }
        self.cur.filter(|(_, (hi, _))| below_hi(k, hi)).map(|(_, (_, v))| *v)
    }
}

static MAX_TREE_BUILT_BLOCKS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn rt_max_tree_built_blocks_total() -> u64 {
    MAX_TREE_BUILT_BLOCKS.load(std::sync::atomic::Ordering::Relaxed)
}

#[derive(Debug, Clone)]
pub struct MaxTree {
    n: usize,
    size: usize,
    nodes: Box<[u64]>,
}

impl MaxTree {
    pub fn build(n: usize, leaf: impl Fn(usize) -> u64) -> Self {
        let size = n.next_power_of_two().max(1);
        let mut tree = MaxTree { n, size, nodes: vec![0; size].into_boxed_slice() };
        for x in (1..size).rev() {
            let m = tree.value(2 * x, &leaf).max(tree.value(2 * x + 1, &leaf));
            tree.nodes[x] = m;
        }
        MAX_TREE_BUILT_BLOCKS.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
        tree
    }

    pub fn blocks(&self) -> usize {
        self.n
    }

    pub fn resident_bytes(&self) -> usize {
        self.nodes.len() * std::mem::size_of::<u64>()
    }

    fn value(&self, node: usize, leaf: &impl Fn(usize) -> u64) -> u64 {
        if node >= self.size {
            let i = node - self.size;
            if i < self.n { leaf(i) } else { 0 }
        } else {
            self.nodes[node]
        }
    }

    pub fn first_at_least(&self, from: usize, to: usize, h: u64, leaf: impl Fn(usize) -> u64, probes: &mut u64) -> usize {
        let to = to.min(self.n);
        if from >= to {
            return to;
        }
        self.descend(1, 0, self.size, from, to, h, &leaf, probes).unwrap_or(to)
    }

    pub fn last_at_least(&self, from: usize, to: usize, h: u64, leaf: impl Fn(usize) -> u64, probes: &mut u64) -> Option<usize> {
        let to = to.min(self.n);
        if from >= to {
            return None;
        }
        self.descend_rev(1, 0, self.size, from, to, h, &leaf, probes)
    }

    #[allow(clippy::too_many_arguments)]
    fn descend_rev(
        &self,
        node: usize,
        nlo: usize,
        nhi: usize,
        from: usize,
        to: usize,
        h: u64,
        leaf: &impl Fn(usize) -> u64,
        probes: &mut u64,
    ) -> Option<usize> {
        if nhi <= from || nlo >= to {
            return None;
        }
        *probes += 1;
        if self.value(node, leaf) < h {
            return None;
        }
        if node >= self.size {
            return Some(nlo);
        }
        let mid = nlo + (nhi - nlo) / 2;
        self.descend_rev(2 * node + 1, mid, nhi, from, to, h, leaf, probes)
            .or_else(|| self.descend_rev(2 * node, nlo, mid, from, to, h, leaf, probes))
    }

    #[allow(clippy::too_many_arguments)]
    fn descend(
        &self,
        node: usize,
        nlo: usize,
        nhi: usize,
        from: usize,
        to: usize,
        h: u64,
        leaf: &impl Fn(usize) -> u64,
        probes: &mut u64,
    ) -> Option<usize> {
        if nhi <= from || nlo >= to {
            return None;
        }
        *probes += 1;
        if self.value(node, leaf) < h {
            return None;
        }
        if node >= self.size {
            return Some(nlo);
        }
        let mid = nlo + (nhi - nlo) / 2;
        self.descend(2 * node, nlo, mid, from, to, h, leaf, probes)
            .or_else(|| self.descend(2 * node + 1, mid, nhi, from, to, h, leaf, probes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_tree_finds_the_first_block_at_or_above_a_bound_in_logarithmic_steps() {
        let mut r = Rng(0x5eed);
        for n in [0usize, 1, 2, 3, 5, 8, 13, 64, 100, 1000] {
            let leaves: Vec<u64> = (0..n).map(|_| r.below(50)).collect();
            let tree = MaxTree::build(n, |i| leaves[i]);
            for _ in 0..200 {
                let from = r.below(n as u64 + 2) as usize;
                let to = r.below(n as u64 + 2) as usize;
                let h = r.below(55);
                let mut probes = 0;
                let got = tree.first_at_least(from, to, h, |i| leaves[i], &mut probes);
                let end = to.min(n);
                let want = if from >= end { end } else { (from..end).find(|&i| leaves[i] >= h).unwrap_or(end) };
                assert_eq!(got, want, "n {n} [{from}, {to}) h {h}");
                let depth = (usize::BITS - n.next_power_of_two().max(1).leading_zeros()) as u64;
                assert!(probes <= 4 * depth + 2, "n {n}: {probes} probes for depth {depth}");
                probes = 0;
                let got = tree.last_at_least(from, to, h, |i| leaves[i], &mut probes);
                let want = (from..end).rev().find(|&i| leaves[i] >= h);
                assert_eq!(got, want, "reverse n {n} [{from}, {to}) h {h}");
                assert!(probes <= 4 * depth + 2, "reverse n {n}: {probes} probes for depth {depth}");
            }
        }
    }

    fn rec(lo: Option<&[u8]>, hi: Option<&[u8]>, version: u128, seq: u64) -> RtRecord {
        RtRecord { lo: lo.map(<[u8]>::to_vec), hi: hi.map(<[u8]>::to_vec), version, seq }
    }

    #[test]
    fn rt_block_round_trips_every_bound_shape() {
        let records = vec![
            rec(None, None, 1, 0),
            rec(Some(b""), Some(b"b"), 2 << 64, 7),
            rec(Some(b"a"), None, u128::MAX, u64::MAX),
            rec(None, Some(b"zz"), 3, 9),
        ];
        let bytes = encode_rt_block(&records);
        assert_eq!(bytes.len(), rt_block_len(&records));
        assert_eq!(decode_rt_block(&bytes).unwrap(), records);
        assert_eq!(rt_block_len(&[]), 0);
    }

    #[test]
    fn rt_block_rejects_damage_and_malformed_records() {
        let bytes = encode_rt_block(&[rec(Some(b"a"), Some(b"b"), 1, 1)]);
        for i in 0..bytes.len() {
            let mut bad = bytes.clone();
            bad[i] ^= 0x10;
            assert!(decode_rt_block(&bad).is_err(), "flip at {i}");
        }
        assert!(decode_rt_block(&bytes[..bytes.len() - 1]).is_err());
        let reseal = |mut body: Vec<u8>| {
            let crc = crc::crc32(&body);
            body.extend_from_slice(&crc.to_le_bytes());
            body
        };
        let body = |r: &RtRecord| encode_rt_block(std::slice::from_ref(r))[..].split_last_chunk::<4>().unwrap().0.to_vec();
        assert_eq!(
            decode_rt_block(&reseal(body(&rec(Some(b"b"), Some(b"b"), 1, 1)))),
            Err("range tombstone record has an empty span")
        );
        let mut flags = body(&rec(None, None, 1, 1));
        flags[4] = 0x04;
        assert_eq!(decode_rt_block(&reseal(flags)), Err("range tombstone record has unknown flags"));
        let mut absent = body(&rec(Some(b"x"), None, 1, 1));
        absent[4] = 0;
        assert_eq!(decode_rt_block(&reseal(absent)), Err("range tombstone record has bytes for an absent bound"));
        assert_eq!(decode_rt_block(&reseal(0u32.to_le_bytes().to_vec())), Err("range tombstone block count out of range"));
    }

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    const ALPHABET: &[u8] = b"abcd";

    fn all_keys() -> Vec<Vec<u8>> {
        let mut keys = vec![Vec::new()];
        for &a in ALPHABET {
            keys.push(vec![a]);
            for &b in ALPHABET {
                keys.push(vec![a, b]);
            }
        }
        keys.sort();
        keys
    }

    fn random_record(r: &mut Rng, keys: &[Vec<u8>], seq: u64) -> RtRecord {
        loop {
            let lo = (r.below(5) != 0).then(|| keys[r.below(keys.len() as u64) as usize].clone());
            let hi = (r.below(5) != 0).then(|| keys[r.below(keys.len() as u64) as usize].clone());
            let rec = RtRecord { lo, hi, version: 1 + r.below(6) as u128, seq };
            if !rec.span_is_empty() {
                return rec;
            }
        }
    }

    fn model_cover(records: &[RtRecord], k: &[u8]) -> Option<u128> {
        records
            .iter()
            .filter(|r| r.lo.as_deref().is_none_or(|lo| lo <= k) && r.hi.as_deref().is_none_or(|hi| k < hi))
            .map(|r| r.version)
            .max()
    }

    fn probes() -> Vec<Vec<u8>> {
        let mut probes = all_keys();
        for k in all_keys() {
            let mut between = k.clone();
            between.push(0);
            probes.push(between);
            let mut after = k.clone();
            after.push(0xff);
            probes.push(after);
        }
        probes.sort();
        probes.dedup();
        probes
    }

    fn assert_canonical(set: &FragmentSet) {
        let frags: Vec<Fragment> = set.fragments().collect();
        for f in &frags {
            assert!(f.hi.as_deref().is_none_or(|h| f.lo.as_slice() < h), "empty fragment {f:?}");
        }
        for w in frags.windows(2) {
            let hi = w[0].hi.as_deref().expect("only the last fragment may be unbounded above");
            assert!(hi <= w[1].lo.as_slice(), "overlapping fragments {w:?}");
            assert!(!(hi == w[1].lo.as_slice() && w[0].version == w[1].version), "adjacent equal fragments {w:?}");
        }
    }

    #[test]
    fn overlay_matches_the_pointwise_max_model_with_exact_deltas() {
        let keys = all_keys();
        let probes = probes();
        for seed in 1..400u64 {
            let mut r = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
            let mut set = FragmentSet::default();
            let mut applied = Vec::new();
            for seq in 0..(1 + r.below(12)) {
                let rec = random_record(&mut r, &keys, seq);
                let before: Vec<Option<u128>> = probes.iter().map(|k| set.cover(k)).collect();
                let delta = set.overlay(rec.lo.as_deref(), rec.hi.as_deref(), rec.version).unwrap();
                applied.push(rec);
                assert_canonical(&set);
                let mut cursor = set.cursor_from(&[]);
                for (k, before) in probes.iter().zip(before) {
                    let after = set.cover(k);
                    assert_eq!(after, model_cover(&applied, k), "seed {seed} key {k:?}");
                    assert_eq!(cursor.cover(k), after, "seed {seed} cursor at {k:?}");
                    let d: Vec<&CoverDelta> = delta
                        .iter()
                        .filter(|d| d.lo.as_slice() <= k.as_slice() && d.hi.as_deref().is_none_or(|h| k.as_slice() < h))
                        .collect();
                    if after != before {
                        assert_eq!(d.len(), 1, "seed {seed} key {k:?}: one delta piece where the cover rose");
                        assert_eq!((d[0].old, Some(d[0].new)), (before, after), "seed {seed} key {k:?}");
                    } else {
                        assert!(d.is_empty(), "seed {seed} key {k:?}: delta where the cover did not rise");
                    }
                }
            }
        }
    }

    #[test]
    fn overlay_is_a_join_in_any_order_and_idempotent() {
        let keys = all_keys();
        for seed in 1..300u64 {
            let mut r = Rng(seed.wrapping_mul(0xD1B5_4A32_D192_ED03) | 1);
            let records: Vec<RtRecord> = (0..(2 + r.below(8))).map(|seq| random_record(&mut r, &keys, seq)).collect();
            let reference = FragmentSet::from_records(&records);
            for _ in 0..4 {
                let mut order = records.clone();
                for i in (1..order.len()).rev() {
                    order.swap(i, r.below(i as u64 + 1) as usize);
                }
                let split = r.below(order.len() as u64 + 1) as usize;
                let mut left = FragmentSet::from_records(&order[..split]);
                let right = FragmentSet::from_records(&order[split..]);
                for f in right.fragments() {
                    left.overlay(Some(&f.lo), f.hi.as_deref(), f.version).unwrap();
                }
                assert_eq!(left, reference, "seed {seed}: (A ⊕ B) equals the in-order overlay");
            }
            let mut again = reference.clone();
            for rec in &records {
                assert!(again.overlay(rec.lo.as_deref(), rec.hi.as_deref(), rec.version).unwrap().is_empty());
            }
            assert_eq!(again, reference, "seed {seed}: re-applying changes nothing");
        }
    }

    fn contains(outer: &RtRecord, inner: &RtRecord) -> bool {
        outer.lo.as_deref().unwrap_or(&[]) <= inner.lo.as_deref().unwrap_or(&[]) && hi_at_least(outer.hi.as_deref(), inner.hi.as_deref())
    }

    #[test]
    fn drop_dominated_removes_exactly_the_records_a_higher_accounted_record_contains() {
        let keys = all_keys();
        let mut dropped_total = 0;
        for seed in 1..600u64 {
            let mut r = Rng(seed.wrapping_mul(0xA24B_AED4_963E_E407) | 1);
            let n = 1 + r.below(24);
            let records: Vec<RtRecord> = (0..n).map(|_| { let seq = r.below(n + 4); random_record(&mut r, &keys, seq) }).collect();
            let through = r.below(n + 6);
            let mut kept = records.clone();
            let dropped = drop_dominated(&mut kept, through);
            dropped_total += dropped;
            assert_eq!(kept.len() + dropped, records.len(), "seed {seed}");
            let mut rest = kept.iter().peekable();
            let mut gone = Vec::new();
            for rec in &records {
                if rest.peek() == Some(&rec) {
                    rest.next();
                } else {
                    gone.push(rec);
                }
            }
            assert!(rest.next().is_none(), "seed {seed}: kept records keep their order");
            assert_eq!(FragmentSet::from_records(&kept), FragmentSet::from_records(&records), "seed {seed}: F is unchanged");
            let accounted = |set: &[RtRecord]| FragmentSet::from_records(set.iter().filter(|x| x.seq <= through));
            assert_eq!(accounted(&kept), accounted(&records), "seed {seed}: A is unchanged");
            for d in &gone {
                assert!(d.seq <= through, "seed {seed}: an unaccounted record is never dropped: {d:?}");
                assert!(
                    kept.iter().any(|k| k.seq <= through && k.version > d.version && contains(k, d)),
                    "seed {seed}: {d:?} has a kept accounted dominator"
                );
            }
            for k in kept.iter().filter(|k| k.seq <= through) {
                assert!(
                    !records.iter().any(|o| o.seq <= through && o.version > k.version && contains(o, k)),
                    "seed {seed}: dominated record {k:?} was kept"
                );
            }
        }
        assert!(dropped_total > 500, "the model exercises drops: {dropped_total}");
    }

    #[test]
    fn cumulative_prefix_deletes_collapse_to_one_fragment() {
        let mut set = FragmentSet::default();
        for i in 1..200u32 {
            let hi = format!("q/{i:06}");
            set.overlay(Some(b"q/"), Some(hi.as_bytes()), i as u128).unwrap();
            assert_eq!(set.len(), 1);
        }
        assert_eq!(set.cover(b"q/000050"), Some(199));
        assert_eq!(set.cover(b"q/000199"), None);
        assert_eq!(set.cover(b"p"), None);
    }

    #[test]
    fn cap_refuses_growth_without_mutating_and_allows_shrinking() {
        let mut set = FragmentSet::with_max_fragments(2);
        set.overlay(Some(b"a"), Some(b"b"), 1).unwrap();
        set.overlay(Some(b"c"), Some(b"d"), 1).unwrap();
        let before = set.clone();
        let err = set.overlay(Some(b"e"), Some(b"f"), 1).unwrap_err();
        assert_eq!(err, FragmentCapExceeded { fragments: 3, max: 2 });
        assert_eq!(set, before);
        assert_eq!(set.overlay(Some(b"a"), Some(b"d"), 1).unwrap().len(), 1, "fills the gap");
        assert_eq!(set.len(), 1);
        set.set_max_fragments(0);
        assert!(set.overlay(None, None, 5).is_ok(), "an overlay that does not grow the set is never refused");
        assert_eq!(set.fragments().collect::<Vec<_>>(), vec![Fragment { lo: Vec::new(), hi: None, version: 5 }]);
    }

    #[test]
    fn rt_value_round_trips_and_rejects_malformed_values() {
        for (lo, hi) in [(None, None), (Some(&b""[..]), Some(&b"x"[..])), (Some(&b"k"[..]), None), (None, Some(&b"z"[..]))] {
            let v = encode_rt_value(lo, hi);
            assert_eq!(decode_rt_value(&v).unwrap(), (lo.map(<[u8]>::to_vec), hi.map(<[u8]>::to_vec)));
            assert!(decode_rt_value(&v[..v.len() - 1]).is_err());
            let mut longer = v.clone();
            longer.push(0);
            assert_eq!(decode_rt_value(&longer), Err("range tombstone record has trailing bytes"));
        }
        assert_eq!(decode_rt_value(&encode_rt_value(Some(b"b"), Some(b"a"))), Err("range tombstone record has an empty span"));
        assert_eq!(decode_rt_value(&[4, 0, 0, 0, 0, 0, 0, 0, 0]), Err("range tombstone record has unknown flags"));
        assert!(decode_rt_value(&[]).is_err());
    }

    #[test]
    fn hlc_is_the_high_half_of_the_packed_version() {
        assert_eq!(version_hlc((7u128 << 64) | 0xffff), 7);
        assert_eq!(version_hlc(u64::MAX as u128), 0);
    }
}
