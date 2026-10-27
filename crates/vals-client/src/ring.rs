use std::collections::{BTreeMap, BTreeSet};

use crate::zone::ZoneId;

pub type NodeId = u32;

pub const DEFAULT_VNODES: u32 = 128;

pub fn hash64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h = (h ^ (h >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    h = (h ^ (h >> 27)).wrapping_mul(0x94d049bb133111eb);
    h ^ (h >> 31)
}

fn vnode_pos(node: NodeId, i: u32) -> u64 {
    let mut buf = [0u8; 8];
    buf[0..4].copy_from_slice(&node.to_le_bytes());
    buf[4..8].copy_from_slice(&i.to_le_bytes());
    hash64(&buf)
}

#[derive(Debug, Clone)]
pub struct Ring {
    rf: usize,
    vnodes: u32,
    weights: BTreeMap<NodeId, u32>,
    ring: Vec<(u64, NodeId)>,
}

impl Ring {
    pub fn new(rf: usize) -> Self {
        Self::with_vnodes(rf, DEFAULT_VNODES)
    }

    pub fn with_vnodes(rf: usize, vnodes: u32) -> Self {
        assert!(rf >= 1, "replication factor must be >= 1");
        assert!(vnodes >= 1, "must have at least one vnode per node");
        Ring {
            rf,
            vnodes,
            weights: BTreeMap::new(),
            ring: Vec::new(),
        }
    }

    #[doc(hidden)]
    pub fn from_members(rf: usize, vnodes: u32, nodes: impl IntoIterator<Item = NodeId>) -> Self {
        let mut r = Self::with_vnodes(rf, vnodes);
        r.weights = nodes.into_iter().map(|n| (n, 1)).collect();
        r.rebuild();
        r
    }

    #[doc(hidden)]
    pub fn from_weighted(
        rf: usize,
        vnodes: u32,
        nodes: impl IntoIterator<Item = (NodeId, u32)>,
    ) -> Self {
        let mut r = Self::with_vnodes(rf, vnodes);
        r.weights = nodes.into_iter().map(|(n, w)| (n, w.max(1))).collect();
        r.rebuild();
        r
    }

    pub fn rf(&self) -> usize {
        self.rf
    }

    pub fn members(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.weights.keys().copied()
    }

    pub fn members_weighted(&self) -> impl Iterator<Item = (NodeId, u32)> + '_ {
        self.weights.iter().map(|(&n, &w)| (n, w))
    }

    pub fn member_weight(&self, node: NodeId) -> u32 {
        self.weights.get(&node).copied().unwrap_or(0)
    }

    pub fn len(&self) -> usize {
        self.weights.len()
    }

    pub fn is_empty(&self) -> bool {
        self.weights.is_empty()
    }

    pub fn add_node(&mut self, node: NodeId) -> bool {
        if self.weights.contains_key(&node) {
            return false;
        }
        self.weights.insert(node, 1);
        self.rebuild();
        true
    }

    #[doc(hidden)]
    pub fn add_node_weighted(&mut self, node: NodeId, weight: u32) -> bool {
        let weight = weight.max(1);
        if self.weights.get(&node) == Some(&weight) {
            return false;
        }
        self.weights.insert(node, weight);
        self.rebuild();
        true
    }

    #[doc(hidden)]
    pub fn remove_node(&mut self, node: NodeId) -> bool {
        let removed = self.weights.remove(&node).is_some();
        if removed {
            self.rebuild();
        }
        removed
    }

    #[doc(hidden)]
    pub fn set_members(&mut self, nodes: impl IntoIterator<Item = NodeId>) {
        self.weights = nodes.into_iter().map(|n| (n, 1)).collect();
        self.rebuild();
    }

    pub fn set_members_weighted(&mut self, nodes: impl IntoIterator<Item = (NodeId, u32)>) {
        self.weights = nodes.into_iter().map(|(n, w)| (n, w.max(1))).collect();
        self.rebuild();
    }

    pub fn vnodes(&self) -> u32 {
        self.vnodes
    }

    pub fn reconfigure(
        &mut self,
        rf: usize,
        vnodes: u32,
        members: impl IntoIterator<Item = (NodeId, u32)>,
    ) {
        assert!(rf >= 1, "replication factor must be >= 1");
        assert!(vnodes >= 1, "must have at least one vnode per node");
        self.rf = rf;
        self.vnodes = vnodes;
        self.weights = members.into_iter().map(|(n, w)| (n, w.max(1))).collect();
        self.rebuild();
    }

    fn rebuild(&mut self) {
        let total_vnodes: usize = self
            .weights
            .values()
            .map(|&w| w as usize * self.vnodes as usize)
            .sum();
        let mut ring = Vec::with_capacity(total_vnodes);
        for (&node, &weight) in &self.weights {
            for i in 0..weight.saturating_mul(self.vnodes) {
                ring.push((vnode_pos(node, i), node));
            }
        }
        ring.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        self.ring = ring;
    }

    pub fn primary(&self, key: &[u8]) -> Option<NodeId> {
        self.replicas(key).into_iter().next()
    }

    pub fn replicas(&self, key: &[u8]) -> Vec<NodeId> {
        self.replicas_n(key, self.rf)
    }

    pub fn replicas_n(&self, key: &[u8], n: usize) -> Vec<NodeId> {
        self.walk(hash64(key), n, None)
    }

    pub fn replicas_in_zone(
        &self,
        zone_id: ZoneId,
        key: &[u8],
        rf: usize,
        scope: Option<&BTreeSet<NodeId>>,
    ) -> Vec<NodeId> {
        let mut salted = Vec::with_capacity(4 + key.len());
        salted.extend_from_slice(&zone_id.to_le_bytes());
        salted.extend_from_slice(key);
        self.walk(hash64(&salted), rf, scope)
    }

    fn walk(&self, pos: u64, want: usize, scope: Option<&BTreeSet<NodeId>>) -> Vec<NodeId> {
        let avail = match scope {
            None => self.weights.len(),
            Some(s) => self.weights.keys().filter(|n| s.contains(n)).count(),
        };
        let want = want.min(avail);
        if want == 0 || self.ring.is_empty() {
            return Vec::new();
        }
        let start = self.ring.partition_point(|&(p, _)| p < pos);

        let mut out: Vec<NodeId> = Vec::with_capacity(want);
        for step in 0..self.ring.len() {
            let (_, node) = self.ring[(start + step) % self.ring.len()];
            if let Some(s) = scope {
                if !s.contains(&node) {
                    continue;
                }
            }
            if !out.contains(&node) {
                out.push(node);
                if out.len() == want {
                    break;
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn keys(n: usize) -> Vec<Vec<u8>> {
        (0..n).map(|i| format!("key-{i:05}").into_bytes()).collect()
    }

    #[test]
    fn empty_ring_has_no_placement() {
        let r = Ring::with_vnodes(3, 16);
        assert!(r.is_empty());
        assert_eq!(r.primary(b"k"), None);
        assert!(r.replicas(b"k").is_empty());
    }

    #[test]
    fn single_node_owns_everything() {
        let r = Ring::from_members(3, 16, [7]);
        for k in keys(50) {
            assert_eq!(r.replicas(&k), vec![7]);
            assert_eq!(r.primary(&k), Some(7));
        }
    }

    #[test]
    fn replicas_are_rf_distinct_nodes_primary_first() {
        let r = Ring::from_members(3, 64, [1, 2, 3, 4, 5]);
        for k in keys(200) {
            let reps = r.replicas(&k);
            assert_eq!(reps.len(), 3, "exactly RF replicas with 5 members");
            let uniq: BTreeSet<_> = reps.iter().copied().collect();
            assert_eq!(uniq.len(), 3, "replicas are distinct");
            assert_eq!(r.primary(&k), Some(reps[0]), "primary is first in the list");
        }
    }

    #[test]
    fn fewer_members_than_rf_returns_all() {
        let r = Ring::from_members(5, 16, [1, 2]);
        for k in keys(20) {
            let reps = r.replicas(&k);
            assert_eq!(reps.len(), 2);
            assert_eq!(reps.iter().copied().collect::<BTreeSet<_>>(), [1, 2].into());
        }
    }

    #[test]
    fn layout_is_deterministic_regardless_of_insertion_order() {
        let a = Ring::from_members(3, 64, [1, 2, 3, 4, 5]);
        let mut b = Ring::with_vnodes(3, 64);
        for n in [5, 3, 1, 4, 2] {
            b.add_node(n);
        }
        let mut c = Ring::with_vnodes(3, 64);
        c.set_members([3, 1, 4, 2, 5]);
        for k in keys(300) {
            let ra = a.replicas(&k);
            assert_eq!(ra, b.replicas(&k));
            assert_eq!(ra, c.replicas(&k));
        }
    }

    #[test]
    fn primaries_are_roughly_balanced() {
        let nodes = [1, 2, 3, 4];
        let r = Ring::from_members(3, DEFAULT_VNODES, nodes);
        let mut counts: HashMap<NodeId, usize> = HashMap::new();
        let n = 8000;
        for k in keys(n) {
            *counts.entry(r.primary(&k).unwrap()).or_default() += 1;
        }
        let fair = n / nodes.len();
        for node in nodes {
            let c = counts[&node];
            assert!(c > fair / 2, "node {node} under-loaded: {c} of fair {fair}");
            assert!(c < fair * 2, "node {node} over-loaded: {c} of fair {fair}");
        }
    }

    #[test]
    fn weight_one_matches_unweighted() {
        let plain = Ring::from_members(3, 64, [1, 2, 3, 4, 5]);
        let weighted = Ring::from_weighted(3, 64, [(1, 1), (2, 1), (3, 1), (4, 1), (5, 1)]);
        for k in keys(300) {
            assert_eq!(plain.replicas(&k), weighted.replicas(&k));
        }
        assert_eq!(plain.member_weight(3), 1);
    }

    #[test]
    fn higher_weight_owns_proportionally_more() {
        let r = Ring::from_weighted(1, DEFAULT_VNODES, [(1, 4), (2, 1), (3, 1), (4, 1)]);
        let mut counts: HashMap<NodeId, usize> = HashMap::new();
        let n = 12000;
        for k in keys(n) {
            *counts.entry(r.primary(&k).unwrap()).or_default() += 1;
        }
        let share1 = counts[&1] as f64 / n as f64;
        assert!(
            (0.45..0.65).contains(&share1),
            "weight-4 node should own ~4/7: got {share1:.3}"
        );
        for node in [2, 3, 4] {
            let s = counts[&node] as f64 / n as f64;
            assert!(
                s < share1 / 2.0,
                "weight-1 node {node} owns too much: {s:.3}"
            );
        }
    }

    #[test]
    fn weighted_layout_is_deterministic() {
        let a = Ring::from_weighted(3, 64, [(1, 3), (2, 1), (3, 5)]);
        let mut b = Ring::with_vnodes(3, 64);
        b.set_members_weighted([(3, 5), (1, 3), (2, 1)]);
        let mut c = Ring::with_vnodes(3, 64);
        c.add_node_weighted(2, 1);
        c.add_node_weighted(3, 5);
        c.add_node_weighted(1, 3);
        for k in keys(300) {
            let ra = a.replicas(&k);
            assert_eq!(ra, b.replicas(&k));
            assert_eq!(ra, c.replicas(&k));
        }
    }

    #[test]
    fn updating_weight_reshapes_the_ring() {
        let mut r = Ring::from_members(2, 64, [1, 2]);
        assert_eq!(r.member_weight(1), 1);
        assert!(r.add_node_weighted(1, 8), "weight change rebuilds");
        assert_eq!(r.member_weight(1), 8);
        assert!(!r.add_node_weighted(1, 8), "same weight is a no-op");
        let mut n1 = 0;
        let total = 4000;
        for k in keys(total) {
            if r.primary(&k) == Some(1) {
                n1 += 1;
            }
        }
        assert!(n1 as f64 / total as f64 > 0.7, "weight-8 vs weight-1: {n1}");
    }

    #[test]
    fn add_remove_membership_bookkeeping() {
        let mut r = Ring::with_vnodes(3, 16);
        assert!(r.add_node(1));
        assert!(!r.add_node(1), "re-adding is a no-op");
        assert!(r.add_node(2));
        assert_eq!(r.len(), 2);
        assert!(r.remove_node(1));
        assert!(!r.remove_node(1), "removing absent is a no-op");
        assert_eq!(r.members().collect::<Vec<_>>(), vec![2]);
    }

    #[test]
    fn zone_salt_spreads_the_same_key_independently() {
        let r = Ring::from_members(3, DEFAULT_VNODES, [1, 2, 3, 4, 5]);
        let mut differ = 0;
        let total = 500;
        for k in keys(total) {
            let a = r.replicas_in_zone(1, &k, 3, None);
            let b = r.replicas_in_zone(2, &k, 3, None);
            assert_eq!(a.len(), 3);
            assert_eq!(b.len(), 3);
            if a != b {
                differ += 1;
            }
        }
        assert!(
            differ > total * 80 / 100,
            "zones should spread independently: only {differ}/{total} differ"
        );
    }

    #[test]
    fn zone_placement_is_deterministic() {
        let a = Ring::from_members(3, 64, [1, 2, 3, 4, 5]);
        let mut b = Ring::with_vnodes(3, 64);
        b.set_members([5, 4, 3, 2, 1]);
        for k in keys(200) {
            assert_eq!(
                a.replicas_in_zone(7, &k, 3, None),
                b.replicas_in_zone(7, &k, 3, None)
            );
        }
    }

    #[test]
    fn per_zone_rf_is_honored() {
        let r = Ring::from_members(5, DEFAULT_VNODES, [1, 2, 3, 4, 5]);
        for k in keys(100) {
            assert_eq!(
                r.replicas_in_zone(3, &k, 1, None).len(),
                1,
                "rf=1 -> one node"
            );
            assert_eq!(
                r.replicas_in_zone(3, &k, 2, None).len(),
                2,
                "rf=2 -> two nodes"
            );
            assert_eq!(r.replicas_in_zone(3, &k, 9, None).len(), 5);
        }
    }

    #[test]
    fn scope_restricts_placement_to_the_subset() {
        let r = Ring::from_members(3, DEFAULT_VNODES, [1, 2, 3, 4, 5]);
        let scope: BTreeSet<NodeId> = [1, 2, 3].into_iter().collect();
        for k in keys(300) {
            let reps = r.replicas_in_zone(4, &k, 3, Some(&scope));
            assert_eq!(reps.len(), 3, "three in-scope nodes available");
            for n in &reps {
                assert!(scope.contains(n), "node {n} must be in scope");
            }
            let uniq: BTreeSet<_> = reps.iter().copied().collect();
            assert_eq!(uniq.len(), 3, "distinct");
        }
    }

    #[test]
    fn scope_caps_rf_at_in_scope_member_count() {
        let r = Ring::from_members(3, DEFAULT_VNODES, [1, 2, 3, 4, 5]);
        let scope: BTreeSet<NodeId> = [1, 2].into_iter().collect();
        for k in keys(50) {
            let reps = r.replicas_in_zone(4, &k, 3, Some(&scope));
            assert_eq!(reps.iter().copied().collect::<BTreeSet<_>>(), scope);
        }
    }

    #[test]
    fn removing_a_node_disrupts_minimally() {
        let before = Ring::from_members(3, DEFAULT_VNODES, [1, 2, 3, 4, 5]);
        let mut after = before.clone();
        after.remove_node(3);

        let mut changed = 0;
        let total = 2000;
        for k in keys(total) {
            let b = before.replicas(&k);
            let a = after.replicas(&k);
            assert!(!a.contains(&3), "removed node must not appear");
            if b.contains(&3) {
                changed += 1;
                for n in b.iter().filter(|&&n| n != 3) {
                    assert!(a.contains(n), "surviving replica {n} must be retained");
                }
            } else {
                assert_eq!(a, b, "lists without the removed node are unchanged");
            }
        }
        assert!(
            changed > total * 45 / 100 && changed < total * 75 / 100,
            "disruption near RF/N: {changed}/{total}"
        );
    }
}
