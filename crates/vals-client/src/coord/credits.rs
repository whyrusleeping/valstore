use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientBudgets {
    pub max_ops: usize,
    pub max_bytes: usize,
    pub node_max_ops: usize,
    pub node_max_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CreditStats {
    pub in_flight_ops: usize,
    pub in_flight_bytes: usize,
    pub max_ops: usize,
    pub max_bytes: usize,
    pub node_max_ops: usize,
    pub node_max_bytes: usize,
    pub waiters: usize,
    pub blocked_total: u64,
    pub peak_ops: usize,
}

#[doc(hidden)]
pub struct CreditPool {
    max_ops: usize,
    max_bytes: usize,
    pub(super) node_max_ops: usize,
    pub(super) node_max_bytes: usize,
    ops: usize,
    bytes: usize,
    node_in_flight: HashMap<NodeId, (usize, usize)>,
    next_ticket: u64,
    #[doc(hidden)]
    pub waiters: BTreeMap<u64, Waiter>,
    blocked_total: u64,
    peak_ops: usize,
}

#[doc(hidden)]
pub struct Waiter {
    ops: usize,
    bytes: usize,
    node_costs: Vec<NodeCreditCost>,
    deadline_at: Option<std::time::Instant>,
    waker: Waker,
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeCreditCost {
    pub node: NodeId,
    pub ops: usize,
    pub bytes: usize,
}

impl CreditPool {
    #[doc(hidden)]
    pub fn new(max_ops: usize, max_bytes: usize) -> Self {
        CreditPool {
            max_ops,
            max_bytes,
            node_max_ops: usize::MAX,
            node_max_bytes: usize::MAX,
            ops: 0,
            bytes: 0,
            node_in_flight: HashMap::new(),
            next_ticket: 0,
            waiters: BTreeMap::new(),
            blocked_total: 0,
            peak_ops: 0,
        }
    }

    pub(crate) fn from_budgets(limits: ClientBudgets) -> Self {
        let mut pool = Self::new(limits.max_ops, limits.max_bytes);
        pool.set_node_limits(limits.node_max_ops, limits.node_max_bytes);
        pool
    }

    #[doc(hidden)]
    pub fn fits(&self, ops: usize, bytes: usize) -> bool {
        self.fits_global(ops, bytes)
    }

    #[doc(hidden)]
    pub fn set_node_limits(&mut self, max_ops: usize, max_bytes: usize) {
        self.node_max_ops = max_ops;
        self.node_max_bytes = max_bytes;
    }

    fn possible(&self, node_costs: &[NodeCreditCost], ops: usize, bytes: usize) -> bool {
        ops <= self.max_ops
            && bytes <= self.max_bytes
            && node_costs
                .iter()
                .all(|cost| cost.ops <= self.node_max_ops && cost.bytes <= self.node_max_bytes)
    }

    fn fits_global(&self, ops: usize, bytes: usize) -> bool {
        self.ops
            .checked_add(ops)
            .is_some_and(|sum| sum <= self.max_ops)
            && self
                .bytes
                .checked_add(bytes)
                .is_some_and(|sum| sum <= self.max_bytes)
    }

    fn fits_node_one(&self, cost: NodeCreditCost) -> bool {
        let (no, nb) = self
            .node_in_flight
            .get(&cost.node)
            .copied()
            .unwrap_or((0, 0));
        no.checked_add(cost.ops)
            .is_some_and(|sum| sum <= self.node_max_ops)
            && nb
                .checked_add(cost.bytes)
                .is_some_and(|sum| sum <= self.node_max_bytes)
    }

    fn fits_nodes(&self, node_costs: &[NodeCreditCost]) -> bool {
        node_costs.iter().copied().all(|cost| self.fits_node_one(cost))
    }

    pub(crate) fn fits_all(
        &self,
        node_costs: &[NodeCreditCost],
        ops: usize,
        bytes: usize,
    ) -> bool {
        self.fits_global(ops, bytes) && self.fits_nodes(node_costs)
    }

    fn shares_node(a: &[NodeCreditCost], b: &[NodeCreditCost]) -> bool {
        a.iter().any(|left| b.iter().any(|right| left.node == right.node))
    }

    fn can_admit(
        &self,
        ticket: u64,
        node_costs: &[NodeCreditCost],
        ops: usize,
        bytes: usize,
    ) -> bool {
        if !self.fits_all(node_costs, ops, bytes) {
            return false;
        }
        for (_, w) in self.waiters.range(..ticket) {
            let per_node_blocked = self.fits_global(w.ops, w.bytes)
                && !self.fits_nodes(&w.node_costs)
                && !Self::shares_node(node_costs, &w.node_costs);
            if !per_node_blocked {
                return false;
            }
        }
        true
    }

    pub(crate) fn reserve(
        &mut self,
        node_costs: &[NodeCreditCost],
        ops: usize,
        bytes: usize,
    ) {
        self.ops += ops;
        self.bytes += bytes;
        self.peak_ops = self.peak_ops.max(self.ops);
        for cost in node_costs {
            let e = self.node_in_flight.entry(cost.node).or_insert((0, 0));
            e.0 += cost.ops;
            e.1 += cost.bytes;
        }
    }

    pub(crate) fn release(&mut self, node_costs: &[NodeCreditCost], ops: usize, bytes: usize) {
        self.ops -= ops;
        self.bytes -= bytes;
        for cost in node_costs {
            if let Some(e) = self.node_in_flight.get_mut(&cost.node) {
                e.0 -= cost.ops;
                e.1 -= cost.bytes;
                if e.0 == 0 && e.1 == 0 {
                    self.node_in_flight.remove(&cost.node);
                }
            }
        }
        self.wake_one();
    }

    fn wake_one(&self) {
        for (&t, w) in self.waiters.iter() {
            if self.can_admit(t, &w.node_costs, w.ops, w.bytes) {
                w.waker.wake_by_ref();
                return;
            }
        }
        if let Some((_, w)) = self.waiters.iter().next() {
            w.waker.wake_by_ref();
        }
    }

    pub(crate) fn expired_wakers(&self, now: std::time::Instant) -> Vec<Waker> {
        self.waiters
            .values()
            .filter(|waiter| waiter.deadline_at.is_some_and(|deadline| deadline <= now))
            .map(|waiter| waiter.waker.clone())
            .collect()
    }

    pub(crate) fn stats(&self) -> CreditStats {
        CreditStats {
            in_flight_ops: self.ops,
            in_flight_bytes: self.bytes,
            max_ops: self.max_ops,
            max_bytes: self.max_bytes,
            node_max_ops: self.node_max_ops,
            node_max_bytes: self.node_max_bytes,
            waiters: self.waiters.len(),
            blocked_total: self.blocked_total,
            peak_ops: self.peak_ops,
        }
    }
}

pub(super) fn batch_cost(writes: &[(&[u8], &[u8])]) -> (usize, usize) {
    (
        writes.len(),
        writes.iter().fold(0usize, |sum, (key, value)| {
            sum.saturating_add(key.len()).saturating_add(value.len())
        }),
    )
}

#[doc(hidden)]
pub struct Credit {
    pub(crate) pool: Rc<RefCell<CreditPool>>,
    pub(crate) ops: usize,
    pub(crate) bytes: usize,
    pub(crate) node_costs: Vec<NodeCreditCost>,
}

impl Credit {
    pub(crate) fn split_ops_bytes(mut self) -> (Credit, Credit) {
        let bytes = self.bytes;
        let mut byte_node_costs = self.node_costs.clone();
        for cost in &mut self.node_costs {
            cost.bytes = 0;
        }
        for cost in &mut byte_node_costs {
            cost.ops = 0;
        }
        self.bytes = 0;
        let byte_credit = Credit {
            pool: self.pool.clone(),
            ops: 0,
            bytes,
            node_costs: byte_node_costs,
        };
        (self, byte_credit)
    }
}

impl Drop for Credit {
    fn drop(&mut self) {
        self.pool
            .borrow_mut()
            .release(&self.node_costs, self.ops, self.bytes);
    }
}

#[doc(hidden)]
pub struct Acquire {
    pub pool: Rc<RefCell<CreditPool>>,
    pub ops: usize,
    pub bytes: usize,
    pub node_costs: Vec<NodeCreditCost>,
    pub ticket: Option<u64>,
    pub deadline_at: Option<std::time::Instant>,
}

impl Future for Acquire {
    type Output = Result<Credit, CoordError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();
        let mut p = me.pool.borrow_mut();
        if !p.possible(&me.node_costs, me.ops, me.bytes) {
            return Poll::Ready(Err(CoordError::Overloaded));
        }
        if me.deadline_at.is_some_and(|deadline| deadline <= std::time::Instant::now()) {
            if let Some(ticket) = me.ticket {
                p.waiters.remove(&ticket);
                p.wake_one();
            }
            return Poll::Ready(Err(CoordError::Overloaded));
        }
        let ticket = *me.ticket.get_or_insert_with(|| {
            let t = p.next_ticket;
            p.next_ticket += 1;
            t
        });
        if p.can_admit(ticket, &me.node_costs, me.ops, me.bytes) {
            p.reserve(&me.node_costs, me.ops, me.bytes);
            p.waiters.remove(&ticket);
            p.wake_one();
            return Poll::Ready(Ok(Credit {
                pool: me.pool.clone(),
                ops: me.ops,
                bytes: me.bytes,
                node_costs: me.node_costs.clone(),
            }));
        }
        if !p.waiters.contains_key(&ticket) {
            p.blocked_total += 1;
            crate::observe::counters().credit_blocked.inc();
        }
        p.waiters.insert(
            ticket,
            Waiter {
                ops: me.ops,
                bytes: me.bytes,
                node_costs: me.node_costs.clone(),
                deadline_at: me.deadline_at,
                waker: cx.waker().clone(),
            },
        );
        Poll::Pending
    }
}

impl Drop for Acquire {
    fn drop(&mut self) {
        let Some(ticket) = self.ticket else {
            return;
        };
        let mut p = self.pool.borrow_mut();
        if p.waiters.remove(&ticket).is_some() {
            p.wake_one();
        }
    }
}
