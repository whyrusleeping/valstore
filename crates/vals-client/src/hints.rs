use std::collections::{HashMap, VecDeque};

use crate::hlc::Version;
use crate::ring::NodeId;

pub const DEFAULT_HINT_CAP: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hint {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub version: Version,
}

pub trait HintStore {
    fn record(&mut self, node: NodeId, hint: Hint);
    fn drain(&mut self, node: NodeId) -> Vec<Hint>;
    fn restore(&mut self, node: NodeId, hints: Vec<Hint>);
    fn pending(&self, node: NodeId) -> usize;
}

pub struct MemHintStore {
    cap: usize,
    queues: HashMap<NodeId, VecDeque<Hint>>,
}

impl MemHintStore {
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
