use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use vals_rt::{Either, LocalExecutor};

use crate::reactor::{self, Notify, Timer};

use super::{BatchResult, CoordError, Coordinator, Transport, WriteOptions, ZoneId, DEFAULT_ZONE};

async fn proactive_refresh_loop<T: Transport>(
    coord: Rc<Coordinator<T>>,
    period: Duration,
    stop: Rc<Notify>,
) {
    let timer = match Timer::every(period) {
        Ok(t) => t,
        Err(e) => {
            log::error!(
                "coordinator: timerfd creation failed ({e}); proactive ring refresh \
                 DISABLED — the ring heals only on a stale-ring refusal"
            );
            return;
        }
    };
    loop {
        match vals_rt::race(timer.tick(), stop.notified()).await {
            Either::Left(()) => {
                crate::observe::counters().proactive_ring_refreshes.inc();
                coord.adopt_from_any_member().await;
            }
            Either::Right(()) => return,
        }
    }
}

async fn hedge_tick_loop(clock: Rc<crate::HedgeClock>, period: Duration, stop: Rc<Notify>) {
    let timer = match Timer::every(period) {
        Ok(t) => t,
        Err(e) => {
            log::error!(
                "coordinator: timerfd creation failed ({e}); digest hedge ticker \
                 DISABLED — digest reads escalate only on a failed probe"
            );
            return;
        }
    };
    loop {
        match vals_rt::race(timer.tick(), stop.notified()).await {
            Either::Left(()) => clock.advance(),
            Either::Right(()) => return,
        }
    }
}

/// A [`Coordinator`] hosted on a persistent [`LocalExecutor`]; background tasks
/// (ring refresh, hedge ticker) advance as passengers of each foreground op.
///
/// Like the coordinator, the service is neither `Send` nor `Sync`: create and
/// use it on one thread. Background tasks make progress only while that
/// thread is inside [`run`](Self::run) or a `submit_*` call, so an idle
/// service does not refresh its ring. Dropping the service stops the
/// background tasks and runs the executor until every spawned task finishes,
/// including submitted batches.
///
/// ```no_run
/// # use std::time::Duration;
/// # use vals_client::{CoordinatorService, Coordinator};
/// # fn demo<T: vals_client::Transport + 'static>(coord: Coordinator<T>) -> Result<(), vals_client::CoordError> {
/// let svc = CoordinatorService::new(coord).with_proactive_refresh(Duration::from_secs(1));
/// // Each op advances the background refresh loop as a passenger:
/// let coord = svc.coordinator().clone();
/// let version = svc.run(coord.write(b"key", b"value"))?;
/// svc.shutdown();
/// # let _ = version; Ok(())
/// # }
/// ```
pub struct CoordinatorService<T: Transport> {
    ex: LocalExecutor,
    coord: Rc<Coordinator<T>>,
    stop: Rc<Notify>,
}

impl<T: Transport + 'static> CoordinatorService<T> {
    /// Hosts `coord`, starting the digest-hedge ticker if the coordinator was
    /// built [`with_digest_hedge`](Coordinator::with_digest_hedge).
    pub fn new(coord: Coordinator<T>) -> Self {
        let coord = Rc::new(coord);
        let stop = Rc::new(Notify::new());
        let ex = LocalExecutor::new();
        if let (Some(clock), Some(period)) = (coord.hedge_clock(), coord.hedge_tick_period()) {
            ex.spawn(hedge_tick_loop(clock, period, stop.clone()));
        }
        Self { ex, coord, stop }
    }

    /// Also refreshes the ring and catalog from a live member every `period`,
    /// so membership changes are picked up before an operation is refused
    /// for a stale ring.
    pub fn with_proactive_refresh(self, period: Duration) -> Self {
        self.ex.spawn(proactive_refresh_loop(
            self.coord.clone(),
            period,
            self.stop.clone(),
        ));
        self
    }

    /// The hosted coordinator, for building operation futures to pass to
    /// [`run`](Self::run).
    pub fn coordinator(&self) -> &Rc<Coordinator<T>> {
        &self.coord
    }

    /// Blocks the thread running `fut` to completion on the service's
    /// executor (with [`reactor::poll_io`]), advancing the background tasks
    /// meanwhile.
    pub fn run<F: std::future::Future>(&self, fut: F) -> F::Output {
        self.ex.run_until(fut, reactor::poll_io)
    }

    /// [`submit_write_batch_in`](Self::submit_write_batch_in) for the default
    /// zone.
    pub fn submit_write_batch(
        &self,
        writes: Vec<(Vec<u8>, Vec<u8>)>,
        options: impl Into<WriteOptions>,
    ) -> BatchWriteHandle {
        self.submit_write_batch_in(DEFAULT_ZONE, writes, options)
    }

    /// Starts a batch write as a task on the service's executor and returns a
    /// handle to its result.
    ///
    /// The call blocks until the batch has acquired its client credits, then
    /// returns, so callers can pipeline batches while the budget bounds how
    /// many are in flight. The batch keeps running while the service runs
    /// anything else; await the handle inside [`run`](Self::run) to get its
    /// [`BatchResult`]. Dropping the handle does not cancel the batch.
    pub fn submit_write_batch_in(
        &self,
        zone_id: ZoneId,
        writes: Vec<(Vec<u8>, Vec<u8>)>,
        options: impl Into<WriteOptions>,
    ) -> BatchWriteHandle {
        let options = options.into();
        let outcome: Rc<RefCell<Slot<Result<BatchResult, CoordError>>>> =
            Rc::new(RefCell::new(Slot::default()));
        let admitted: Rc<RefCell<Slot<()>>> = Rc::new(RefCell::new(Slot::default()));

        let coord = self.coord.clone();
        let task_outcome = outcome.clone();
        let task_admitted = admitted.clone();
        self.ex.spawn(async move {
            let views: Vec<(&[u8], &[u8])> = writes
                .iter()
                .map(|(k, v)| (k.as_slice(), v.as_slice()))
                .collect();
            let credit = coord.acquire_batch(zone_id, &views, false).await;
            deliver(&task_admitted, ());
            let res = match credit {
                Ok(credit) => {
                    coord
                        .batch_inner(zone_id, &views, options, credit, false)
                        .await
                }
                Err(error) => Err(error),
            };
            deliver(&task_outcome, res);
        });
        self.ex.run_until(SlotFuture { slot: admitted }, reactor::poll_io);
        BatchWriteHandle { slot: outcome }
    }

    /// Stops the background tasks and runs the executor until every spawned
    /// task, including submitted batches, has finished.
    pub fn shutdown(&self) {
        self.stop.signal();
        self.ex.run(reactor::poll_io);
    }
}

impl<T: Transport> Drop for CoordinatorService<T> {
    fn drop(&mut self) {
        self.stop.signal();
        self.ex.run(reactor::poll_io);
    }
}

struct Slot<T> {
    value: Option<T>,
    waker: Option<Waker>,
    taken: bool,
}

impl<T> Default for Slot<T> {
    fn default() -> Self {
        Slot {
            value: None,
            waker: None,
            taken: false,
        }
    }
}

fn deliver<T>(slot: &Rc<RefCell<Slot<T>>>, value: T) {
    let mut s = slot.borrow_mut();
    s.value = Some(value);
    if let Some(w) = s.waker.take() {
        w.wake();
    }
}

struct SlotFuture<T> {
    slot: Rc<RefCell<Slot<T>>>,
}

impl<T> Future for SlotFuture<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut s = self.slot.borrow_mut();
        match s.value.take() {
            Some(v) => Poll::Ready(v),
            None => {
                s.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

/// The pending result of [`CoordinatorService::submit_write_batch`]. Poll it
/// only on the service's thread, inside [`CoordinatorService::run`].
pub struct BatchWriteHandle {
    slot: Rc<RefCell<Slot<Result<BatchResult, CoordError>>>>,
}

impl Future for BatchWriteHandle {
    type Output = Result<BatchResult, CoordError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut s = self.slot.borrow_mut();
        if let Some(v) = s.value.take() {
            s.taken = true;
            return Poll::Ready(v);
        }
        assert!(
            !s.taken,
            "BatchWriteHandle polled again after it already resolved"
        );
        s.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}
