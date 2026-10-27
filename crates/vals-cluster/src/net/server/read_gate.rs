use super::*;

pub(crate) struct ReadGate {
    limit: usize,
    inflight: Cell<usize>,
    pause_depth: Cell<u32>,
    waiters: RefCell<VecDeque<Waker>>,
    drained: reactor::Notify,
}

impl ReadGate {
    pub(crate) fn new() -> ReadGate {
        Self::with_limit(usize::MAX)
    }

    pub(crate) fn with_limit(limit: usize) -> ReadGate {
        assert!(limit > 0);
        ReadGate {
            limit,
            inflight: Cell::new(0),
            pause_depth: Cell::new(0),
            waiters: RefCell::new(VecDeque::new()),
            drained: reactor::Notify::new(),
        }
    }

    pub(crate) fn inflight(&self) -> usize {
        self.inflight.get()
    }

    #[cfg(test)]
    pub(crate) fn paused(&self) -> bool {
        self.pause_depth.get() > 0
    }

    pub(crate) fn acquire(&self) -> AcquireRead<'_> {
        AcquireRead { gate: self }
    }

    pub(super) fn try_acquire(&self) -> Option<InflightGuard<'_>> {
        (self.pause_depth.get() == 0 && self.inflight.get() < self.limit)
            .then(|| InflightGuard::new(self))
    }

    pub(crate) fn pause(&self) -> ReadPause<'_> {
        self.pause_depth.set(self.pause_depth.get() + 1);
        ReadPause { gate: self }
    }

    pub(crate) fn drained(&self) -> reactor::Notified<'_> {
        self.drained.notified()
    }
}

pub(crate) struct AcquireRead<'a> {
    gate: &'a ReadGate,
}

impl<'a> Future for AcquireRead<'a> {
    type Output = InflightGuard<'a>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<InflightGuard<'a>> {
        if self.gate.pause_depth.get() > 0 || self.gate.inflight.get() >= self.gate.limit {
            let mut waiters = self.gate.waiters.borrow_mut();
            if !waiters.iter().any(|waker| waker.will_wake(cx.waker())) {
                waiters.push_back(cx.waker().clone());
            }
            return Poll::Pending;
        }
        Poll::Ready(InflightGuard::new(self.gate))
    }
}

pub(crate) struct InflightGuard<'a> {
    gate: &'a ReadGate,
    active: bool,
}

impl<'a> InflightGuard<'a> {
    fn new(gate: &'a ReadGate) -> InflightGuard<'a> {
        gate.inflight.set(gate.inflight.get() + 1);
        InflightGuard { gate, active: true }
    }

    pub(crate) async fn checkpoint(&mut self) -> bool {
        if self.gate.pause_depth.get() == 0 {
            return false;
        }
        self.release();
        let mut resumed = self.gate.acquire().await;
        resumed.active = false;
        self.active = true;
        true
    }

    fn release(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        let g = self.gate;
        let n = g.inflight.get().saturating_sub(1);
        g.inflight.set(n);
        if n == 0 && g.pause_depth.get() > 0 {
            g.drained.signal();
        }
        if n < g.limit && g.pause_depth.get() == 0 {
            if let Some(waker) = g.waiters.borrow_mut().pop_front() {
                waker.wake();
            }
        }
    }
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.release();
    }
}

pub(crate) struct ReadPause<'a> {
    gate: &'a ReadGate,
}

impl Drop for ReadPause<'_> {
    fn drop(&mut self) {
        let depth = self.gate.pause_depth.get().saturating_sub(1);
        self.gate.pause_depth.set(depth);
        if depth > 0 {
            return;
        }
        let waiters: Vec<Waker> = self.gate.waiters.borrow_mut().drain(..).collect();
        for w in waiters {
            w.wake();
        }
    }
}

