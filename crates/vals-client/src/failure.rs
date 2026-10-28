use std::collections::HashMap;

use crate::ring::NodeId;

/// Settings for [`Coordinator::with_failure_detector`](crate::Coordinator::with_failure_detector).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FdConfig {
    /// Consecutive failures that mark a node down (default 3, at least 1).
    pub failure_threshold: u32,
    /// Milliseconds between probes of a down node (default 1000).
    pub open_cooldown_ms: u64,
}

impl Default for FdConfig {
    fn default() -> Self {
        FdConfig {
            failure_threshold: 3,
            open_cooldown_ms: 1000,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct NodeHealth {
    fails: u32,
    probe_after: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct FailureDetector {
    cfg: FdConfig,
    nodes: HashMap<NodeId, NodeHealth>,
}

impl FailureDetector {
    pub fn new(cfg: FdConfig) -> Self {
        FailureDetector {
            cfg: FdConfig {
                failure_threshold: cfg.failure_threshold.max(1),
                open_cooldown_ms: cfg.open_cooldown_ms,
            },
            nodes: HashMap::new(),
        }
    }

    pub fn admit(&mut self, node: NodeId, now: u64) -> bool {
        let h = self.nodes.entry(node).or_default();
        match h.probe_after {
            None => true,
            Some(t) if now >= t => {
                h.probe_after = Some(now.saturating_add(self.cfg.open_cooldown_ms));
                true
            }
            Some(_) => false,
        }
    }

    pub fn record_success(&mut self, node: NodeId) {
        let h = self.nodes.entry(node).or_default();
        h.fails = 0;
        if h.probe_after.take().is_some() {
            log::info!("failure detector: node {node} recovered (probe succeeded)");
            crate::observe::counters().detector_recoveries.inc();
        }
    }

    pub fn record_failure(&mut self, node: NodeId, now: u64) {
        let cooldown = self.cfg.open_cooldown_ms;
        let threshold = self.cfg.failure_threshold;
        let h = self.nodes.entry(node).or_default();
        h.fails = h.fails.saturating_add(1);
        if h.probe_after.is_none() && h.fails >= threshold {
            log::warn!(
                "failure detector: node {node} tripped open after {} consecutive \
                 failures; fast-failing for {cooldown}ms between probes",
                h.fails
            );
            crate::observe::counters().detector_trips.inc();
        }
        if h.probe_after.is_some() || h.fails >= threshold {
            h.probe_after = Some(now.saturating_add(cooldown));
        }
    }

    pub fn is_down(&self, node: NodeId, now: u64) -> bool {
        match self.nodes.get(&node).and_then(|h| h.probe_after) {
            Some(t) => now < t,
            None => false,
        }
    }

    pub fn down_nodes(&self, now: u64) -> Vec<NodeId> {
        let mut v: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|(_, h)| matches!(h.probe_after, Some(t) if now < t))
            .map(|(&n, _)| n)
            .collect();
        v.sort_unstable();
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fd(threshold: u32, cooldown: u64) -> FailureDetector {
        FailureDetector::new(FdConfig {
            failure_threshold: threshold,
            open_cooldown_ms: cooldown,
        })
    }

    #[test]
    fn unknown_node_is_admitted() {
        let mut d = fd(3, 1000);
        assert!(d.admit(7, 0), "a node never seen is healthy");
        assert!(!d.is_down(7, 0));
    }

    #[test]
    fn trips_open_after_threshold_consecutive_failures() {
        let mut d = fd(3, 1000);
        d.record_failure(1, 0);
        d.record_failure(1, 0);
        assert!(d.admit(1, 0), "2 < 3 failures: still up");
        assert!(!d.is_down(1, 0));
        d.record_failure(1, 10);
        assert!(d.is_down(1, 10));
        assert!(!d.admit(1, 10), "tripped: fast-fail");
        assert!(!d.admit(1, 1009), "still inside cooldown (<10+1000)");
    }

    #[test]
    fn a_success_resets_the_failure_run() {
        let mut d = fd(3, 1000);
        d.record_failure(1, 0);
        d.record_failure(1, 0);
        d.record_success(1);
        d.record_failure(1, 0);
        d.record_failure(1, 0);
        assert!(d.admit(1, 0), "the run was broken, so 2 more is still < 3");
    }

    #[test]
    fn probes_once_after_cooldown_and_recovers_on_success() {
        let mut d = fd(1, 1000);
        d.record_failure(1, 0);
        assert!(!d.admit(1, 500), "in cooldown");
        assert!(d.admit(1, 1000), "probe admitted");
        assert!(
            !d.admit(1, 1000),
            "second concurrent caller fast-fails (one probe)"
        );
        d.record_success(1);
        assert!(d.admit(1, 1000));
        assert!(!d.is_down(1, 1000));
    }

    #[test]
    fn a_failed_probe_re_arms_the_cooldown() {
        let mut d = fd(1, 1000);
        d.record_failure(1, 0);
        assert!(d.admit(1, 1000), "probe at t=1000");
        d.record_failure(1, 1000);
        assert!(!d.admit(1, 1500), "cooldown re-armed to 2000");
        assert!(d.admit(1, 2000), "next probe after the new cooldown");
    }

    #[test]
    fn a_cancelled_probe_does_not_get_stuck_half_open() {
        let mut d = fd(1, 1000);
        d.record_failure(1, 0);
        assert!(d.admit(1, 1000), "probe admitted (then dropped, no record)");
        assert!(
            !d.admit(1, 1500),
            "next caller fast-fails — not stuck open-for-all"
        );
        assert!(d.admit(1, 2000), "re-probes after the bumped cooldown");
    }

    #[test]
    fn down_nodes_lists_only_those_in_cooldown() {
        let mut d = fd(1, 1000);
        d.record_failure(2, 0);
        d.record_failure(5, 0);
        assert_eq!(d.down_nodes(500), vec![2, 5]);
        d.record_success(2);
        assert_eq!(d.down_nodes(600), vec![5]);
        assert_eq!(
            d.down_nodes(2000),
            Vec::<NodeId>::new(),
            "cooldowns elapsed"
        );
    }
}
