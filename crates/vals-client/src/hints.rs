use std::collections::{HashMap, VecDeque};

use crate::hlc::Version;
use crate::ring::NodeId;

/// Hints [`MemHintStore::default`] keeps per node.
pub const DEFAULT_HINT_CAP: usize = 4096;

/// A default-zone write held for a replica that missed it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hint {
    /// The key.
    pub key: Vec<u8>,
    /// The value, or `None` for a delete.
    pub value: Option<Vec<u8>>,
    /// The write's version.
    pub version: Version,
}

/// Where a coordinator keeps hints for unreachable replicas; see
/// [`Coordinator::with_hint_store`](crate::Coordinator::with_hint_store).
///
/// Hints only speed up convergence; anti-entropy repairs replicas without
/// them, so a store may drop hints.
pub trait HintStore {
    /// Keeps `hint` for `node`.
    fn record(&mut self, node: NodeId, hint: Hint);
    /// Removes and returns every hint for `node`, oldest first.
    fn drain(&mut self, node: NodeId) -> Vec<Hint>;
    /// Puts back hints that could not be delivered.
    fn restore(&mut self, node: NodeId, hints: Vec<Hint>);
    /// How many hints are held for `node`.
    fn pending(&self, node: NodeId) -> usize;
}

/// An in-memory [`HintStore`] keeping at most a fixed number of hints per
/// node, dropping the oldest when full.
pub struct MemHintStore {
    cap: usize,
    queues: HashMap<NodeId, VecDeque<Hint>>,
}

impl MemHintStore {
    /// A store keeping up to `cap_per_node` hints per node (at least 1).
    pub fn new(cap_per_node: usize) -> Self {
        MemHintStore {
            cap: cap_per_node.max(1),
            queues: HashMap::new(),
        }
    }

    #[doc(hidden)]
    pub fn queues(&self) -> &HashMap<NodeId, VecDeque<Hint>> {
        &self.queues
    }
}

impl Default for MemHintStore {
    fn default() -> Self {
        Self::new(DEFAULT_HINT_CAP)
    }
}

impl HintStore for MemHintStore {
    fn record(&mut self, node: NodeId, hint: Hint) {
        let q = self.queues.entry(node).or_default();
        if q.len() >= self.cap {
            q.pop_front();
            log::debug!(
                "hints: queue for node {node} saturated at {} — dropped the oldest \
                 hint (anti-entropy is the backstop)",
                self.cap
            );
            crate::observe::counters().hints_dropped.inc();
        }
        q.push_back(hint);
        crate::observe::counters().hints_recorded.inc();
    }

    fn drain(&mut self, node: NodeId) -> Vec<Hint> {
        self.queues.remove(&node).map(Vec::from).unwrap_or_default()
    }

    fn restore(&mut self, node: NodeId, hints: Vec<Hint>) {
        if !hints.is_empty() {
            self.queues.insert(node, VecDeque::from(hints));
        }
    }

    fn pending(&self, node: NodeId) -> usize {
        self.queues.get(&node).map_or(0, VecDeque::len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hint(k: &str, hlc: u64) -> Hint {
        Hint {
            key: k.as_bytes().to_vec(),
            value: Some(b"v".to_vec()),
            version: Version { hlc, node: 1 },
        }
    }

    #[test]
    fn mem_store_is_bounded_evicting_oldest() {
        let mut s = MemHintStore::new(3);
        for i in 0..5 {
            s.record(7, hint(&format!("k{i}"), i));
        }
        assert_eq!(s.pending(7), 3);
        let held = s.drain(7);
        let keys: Vec<_> = held.iter().map(|h| h.key.clone()).collect();
        assert_eq!(keys, vec![b"k2".to_vec(), b"k3".to_vec(), b"k4".to_vec()]);
        assert_eq!(s.pending(7), 0);
    }
}
