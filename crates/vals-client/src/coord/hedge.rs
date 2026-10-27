use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

#[derive(Default)]
pub struct HedgeClock {
    tick: Cell<u64>,
    waiters: RefCell<BTreeMap<u64, Vec<Option<Waker>>>>,
}

impl HedgeClock {
    pub fn new() -> Rc<Self> {
        Rc::new(Self::default())
    }

    pub fn now(&self) -> u64 {
        self.tick.get()
    }

    pub fn advance(&self) {
        let now = self.tick.get() + 1;
        self.tick.set(now);
        let due = {
            let mut waiters = self.waiters.borrow_mut();
            if waiters.is_empty() {
                return;
            }
            let future = waiters.split_off(&(now + 1));
            std::mem::replace(&mut *waiters, future)
        };
        for (_deadline, bucket) in due {
            for waker in bucket.into_iter().flatten() {
                waker.wake();
            }
        }
    }

    pub fn wait_until(self: &Rc<Self>, deadline: u64) -> WaitTicks {
        WaitTicks {
            clock: self.clone(),
            deadline,
            slot: None,
        }
    }

    pub fn wait_ticks(self: &Rc<Self>, n: u64) -> WaitTicks {
        self.wait_until(self.tick.get().saturating_add(n))
    }

    #[cfg(test)]
    fn waiter_count(&self) -> usize {
        self.waiters
            .borrow()
            .values()
            .flat_map(|bucket| bucket.iter())
            .filter(|slot| slot.is_some())
            .count()
    }
}

pub struct WaitTicks {
    clock: Rc<HedgeClock>,
    deadline: u64,
    slot: Option<usize>,
}

impl Future for WaitTicks {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.clock.tick.get() >= this.deadline {
            return Poll::Ready(());
        }
        let mut waiters = this.clock.waiters.borrow_mut();
        let bucket = waiters.entry(this.deadline).or_default();
        match this.slot {
            Some(idx) => match bucket.get_mut(idx).and_then(Option::as_mut) {
                Some(existing) => {
                    if !existing.will_wake(cx.waker()) {
                        existing.clone_from(cx.waker());
                    }
                }
                None => {
                    bucket.push(Some(cx.waker().clone()));
                    this.slot = Some(bucket.len() - 1);
                }
            },
            None => {
                bucket.push(Some(cx.waker().clone()));
                this.slot = Some(bucket.len() - 1);
            }
        }
        Poll::Pending
    }
}

impl Drop for WaitTicks {
    fn drop(&mut self) {
        let Some(idx) = self.slot else { return };
        if let Ok(mut waiters) = self.clock.waiters.try_borrow_mut() {
            if let Some(bucket) = waiters.get_mut(&self.deadline) {
                if let Some(slot) = bucket.get_mut(idx) {
                    *slot = None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::Wake;
    use vals_rt::block_on;

    struct CountingWaker(AtomicUsize);

    impl CountingWaker {
        fn new() -> Arc<Self> {
            Arc::new(Self(AtomicUsize::new(0)))
        }
        fn count(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }

    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn now_and_advance_track_the_tick() {
        let clock = HedgeClock::new();
        assert_eq!(clock.now(), 0);
        clock.advance();
        clock.advance();
        assert_eq!(clock.now(), 2);
    }

    #[test]
    fn wait_ticks_resolves_after_enough_advances() {
        let clock = HedgeClock::new();
        let advance = clock.clone();
        let woken_at = block_on(
            {
                let clock = clock.clone();
                async move {
                    clock.wait_ticks(3).await;
                    clock.now()
                }
            },
            || advance.advance(),
        );
        assert!(woken_at >= 3, "resolved at tick {woken_at}, expected ≥ 3");
    }

    #[test]
    fn wait_zero_ticks_is_immediate() {
        let clock = HedgeClock::new();
        block_on(
            {
                let clock = clock.clone();
                async move { clock.wait_ticks(0).await }
            },
            || panic!("wait_ticks(0) must not park"),
        );
    }

    #[test]
    fn advance_wakes_multiple_parked_waiters() {
        let clock = HedgeClock::new();
        let advance = clock.clone();
        let (a, b) = block_on(
            {
                let clock = clock.clone();
                async move {
                    let w1 = clock.wait_until(2);
                    let w2 = clock.wait_until(2);
                    vals_rt::join_all(vec![
                        Box::pin(async move {
                            w1.await;
                            1
                        }) as Pin<Box<dyn Future<Output = i32>>>,
                        Box::pin(async move {
                            w2.await;
                            2
                        }),
                    ])
                    .await;
                    (clock.now(), clock.now())
                }
            },
            || advance.advance(),
        );
        assert!(a >= 2 && b >= 2);
    }

    #[test]
    fn repolling_pending_wait_registers_o1_waiters() {
        let clock = HedgeClock::new();
        let mut fut = clock.wait_until(5);
        let waker = Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);
        let mut pinned = Pin::new(&mut fut);
        for _ in 0..1000 {
            assert!(pinned.as_mut().poll(&mut cx).is_pending());
            assert_eq!(clock.waiter_count(), 1, "waiter storage grew under re-poll");
        }

        clock.advance();
        assert_eq!(
            clock.now(),
            1,
            "advance moved the tick but not past the deadline"
        );
        assert_eq!(
            clock.waiter_count(),
            1,
            "sub-deadline advance must keep the waiter parked"
        );
        for _ in 0..1000 {
            assert!(pinned.as_mut().poll(&mut cx).is_pending());
            assert_eq!(clock.waiter_count(), 1, "waiter storage grew under re-poll");
        }

        while clock.now() < 5 {
            clock.advance();
        }
        assert!(pinned.as_mut().poll(&mut cx).is_ready());
    }

    #[test]
    fn distinct_waiters_each_register_a_slot() {
        let clock = HedgeClock::new();
        let mut a = clock.wait_until(5);
        let mut b = clock.wait_until(5);
        let waker = Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);
        let (mut pa, mut pb) = (Pin::new(&mut a), Pin::new(&mut b));
        for _ in 0..100 {
            assert!(pa.as_mut().poll(&mut cx).is_pending());
            assert!(pb.as_mut().poll(&mut cx).is_pending());
        }
        assert_eq!(clock.waiter_count(), 2);
    }

    #[test]
    fn dropping_pending_wait_deregisters_its_waker() {
        let clock = HedgeClock::new();
        let cw = CountingWaker::new();
        let waker = Waker::from(cw.clone());
        let mut cx = Context::from_waker(&waker);
        {
            let mut fut = clock.wait_until(3);
            assert!(Pin::new(&mut fut).poll(&mut cx).is_pending());
            assert_eq!(clock.waiter_count(), 1);
        }
        assert_eq!(
            clock.waiter_count(),
            0,
            "drop must deregister the parked waker"
        );
        while clock.now() < 5 {
            clock.advance();
        }
        assert_eq!(cw.count(), 0, "a dropped waiter must never be woken");
    }

    #[test]
    fn advance_wakes_only_due_waiters() {
        let clock = HedgeClock::new();
        let cw_a = CountingWaker::new();
        let cw_b = CountingWaker::new();
        let waker_a = Waker::from(cw_a.clone());
        let waker_b = Waker::from(cw_b.clone());
        let mut cx_a = Context::from_waker(&waker_a);
        let mut cx_b = Context::from_waker(&waker_b);
        let mut a = clock.wait_until(1);
        let mut b = clock.wait_until(5);
        assert!(Pin::new(&mut a).poll(&mut cx_a).is_pending());
        assert!(Pin::new(&mut b).poll(&mut cx_b).is_pending());
        assert_eq!(clock.waiter_count(), 2);

        clock.advance();
        assert_eq!(cw_a.count(), 1, "due waiter woken once");
        assert_eq!(cw_b.count(), 0, "later-deadline waiter must not be woken");
        assert_eq!(clock.waiter_count(), 1, "only the due bucket was drained");
        assert!(Pin::new(&mut a).poll(&mut cx_a).is_ready());

        while clock.now() < 5 {
            clock.advance();
        }
        assert_eq!(cw_b.count(), 1, "farther waiter woken exactly once at deadline");
        assert!(Pin::new(&mut b).poll(&mut cx_b).is_ready());
        assert_eq!(clock.waiter_count(), 0);
    }

    #[test]
    fn completed_reads_leave_no_stale_wakers() {
        let clock = HedgeClock::new();
        let cw = CountingWaker::new();
        let waker = Waker::from(cw.clone());
        let mut cx = Context::from_waker(&waker);
        for _ in 0..10 {
            let mut fut = clock.wait_until(2);
            assert!(Pin::new(&mut fut).poll(&mut cx).is_pending());
        }
        assert_eq!(clock.waiter_count(), 0, "completed reads left stale wakers");

        while clock.now() < 2 {
            clock.advance();
        }
        assert_eq!(cw.count(), 0, "stale wakers spuriously woke the executor");
        assert!(
            clock.waiters.borrow().is_empty(),
            "drained bucket left behind"
        );
    }
}
