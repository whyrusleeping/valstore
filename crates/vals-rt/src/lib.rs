
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Wake, Waker};

type Task = Pin<Box<dyn Future<Output = ()>>>;

struct ReadyTask {
    id: usize,
    notify: Weak<TaskNotify>,
}

#[derive(Default)]
struct ReadyQueue {
    tasks: Mutex<VecDeque<ReadyTask>>,
}

impl ReadyQueue {
    fn push(&self, id: usize, notify: &Arc<TaskNotify>) {
        self.tasks.lock().unwrap().push_back(ReadyTask {
            id,
            notify: Arc::downgrade(notify),
        });
    }

    fn pop(&self) -> Option<ReadyTask> {
        self.tasks.lock().unwrap().pop_front()
    }

    fn len(&self) -> usize {
        self.tasks.lock().unwrap().len()
    }

    fn is_empty(&self) -> bool {
        self.tasks.lock().unwrap().is_empty()
    }
}

struct TaskNotify {
    ready: Arc<ReadyQueue>,
    id: usize,
    active: AtomicBool,
}

impl TaskNotify {
    fn enqueue(self: &Arc<Self>) {
        if self.active.load(Ordering::Acquire) {
            self.ready.push(self.id, self);
        }
    }
}

impl Wake for TaskNotify {
    fn wake(self: Arc<Self>) {
        self.enqueue();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.enqueue();
    }
}

struct TaskEntry {
    fut: Task,
    notify: Arc<TaskNotify>,
}

struct Shared {
    tasks: RefCell<Vec<Option<TaskEntry>>>,
    free: RefCell<Vec<usize>>,
    ready: Arc<ReadyQueue>,
}

impl Shared {
    fn spawn(&self, fut: impl Future<Output = ()> + 'static) {
        let id = match self.free.borrow_mut().pop() {
            Some(id) => id,
            None => self.tasks.borrow().len(),
        };
        let notify = Arc::new(TaskNotify {
            ready: self.ready.clone(),
            id,
            active: AtomicBool::new(true),
        });
        let entry = TaskEntry {
            fut: Box::pin(fut),
            notify: notify.clone(),
        };
        let mut tasks = self.tasks.borrow_mut();
        if id == tasks.len() {
            tasks.push(Some(entry));
        } else {
            tasks[id] = Some(entry);
        }
        drop(tasks);
        self.ready.push(id, &notify);
    }

    fn live(&self) -> usize {
        self.tasks.borrow().iter().filter(|t| t.is_some()).count()
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        for entry in self.tasks.get_mut().iter_mut().filter_map(Option::as_mut) {
            entry.notify.active.store(false, Ordering::Release);
        }
    }
}

struct WakeFlag(AtomicBool);

impl WakeFlag {
    fn new() -> Self {
        Self(AtomicBool::new(true))
    }

    fn take(&self) -> bool {
        self.0.swap(false, Ordering::AcqRel)
    }

    fn is_set(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::Release);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::Release);
    }
}

#[derive(Clone)]
pub struct Spawner {
    shared: Rc<Shared>,
}

impl Spawner {
    pub fn spawn(&self, fut: impl Future<Output = ()> + 'static) {
        self.shared.spawn(fut);
    }
}

pub struct LocalExecutor {
    shared: Rc<Shared>,
}

impl Default for LocalExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalExecutor {
    pub fn new() -> Self {
        let shared = Rc::new(Shared {
            tasks: RefCell::new(Vec::new()),
            free: RefCell::new(Vec::new()),
            ready: Arc::new(ReadyQueue::default()),
        });
        LocalExecutor { shared }
    }

    pub fn spawner(&self) -> Spawner {
        Spawner {
            shared: self.shared.clone(),
        }
    }

    pub fn spawn(&self, fut: impl Future<Output = ()> + 'static) {
        self.shared.spawn(fut);
    }

    fn drain_ready(&self) {
        let mut budget = self.shared.ready.len();
        while budget > 0 {
            budget -= 1;
            let Some(ready) = self.shared.ready.pop() else {
                break;
            };
            let Some(notify) = ready.notify.upgrade() else {
                continue;
            };
            let entry = {
                let mut tasks = self.shared.tasks.borrow_mut();
                let Some(slot) = tasks.get_mut(ready.id) else {
                    continue;
                };
                let Some(entry) = slot.as_ref() else { continue };
                if !Arc::ptr_eq(&entry.notify, &notify) {
                    continue;
                }
                slot.take().unwrap()
            };
            let TaskEntry { mut fut, notify } = entry;
            let waker = Waker::from(notify.clone());
            let mut cx = Context::from_waker(&waker);
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(()) => {
                    notify.active.store(false, Ordering::Release);
                    self.shared.free.borrow_mut().push(ready.id);
                }
                Poll::Pending => {
                    self.shared.tasks.borrow_mut()[ready.id] = Some(TaskEntry { fut, notify });
                }
            }
        }
    }

    pub fn run(&self, mut poll_io: impl FnMut()) {
        loop {
            self.drain_ready();
            if self.shared.live() == 0 {
                return;
            }
            poll_io();
        }
    }

    fn cancel_all(&mut self) -> usize {
        let tasks = std::mem::take(&mut *self.shared.tasks.borrow_mut());
        let canceled = tasks.iter().filter(|entry| entry.is_some()).count();
        for entry in tasks.iter().filter_map(Option::as_ref) {
            entry.notify.active.store(false, Ordering::Release);
        }
        self.shared.free.borrow_mut().clear();
        while self.shared.ready.pop().is_some() {}
        drop(tasks);
        canceled
    }

    pub fn run_until_and_cancel<F: Future>(
        &mut self,
        fut: F,
        mut poll_io: impl FnMut(),
    ) -> (F::Output, usize) {
        let mut fut = Box::pin(fut);
        let woken = Arc::new(WakeFlag::new());
        let fg_waker = Waker::from(woken.clone());
        let mut fg_cx = Context::from_waker(&fg_waker);
        loop {
            self.drain_ready();
            if woken.take() {
                if let Poll::Ready(v) = fut.as_mut().poll(&mut fg_cx) {
                    return (v, self.cancel_all());
                }
            }
            poll_io();
        }
    }

    pub fn run_until<F: Future>(&self, fut: F, mut poll_io: impl FnMut()) -> F::Output {
        let mut fut = Box::pin(fut);
        let woken = Arc::new(WakeFlag::new());
        let fg_waker = Waker::from(woken.clone());
        let mut fg_cx = Context::from_waker(&fg_waker);
        loop {
            self.drain_ready();
            if woken.take() {
                if let Poll::Ready(v) = fut.as_mut().poll(&mut fg_cx) {
                    return v;
                }
            }
            if self.shared.ready.is_empty() && !woken.is_set() {
                poll_io();
            }
        }
    }
}

pub fn block_on<F: Future>(fut: F, mut poll_io: impl FnMut()) -> F::Output {
    let mut fut = Box::pin(fut);
    let woken = Arc::new(WakeFlag::new());
    let waker = Waker::from(woken.clone());
    let mut cx = Context::from_waker(&waker);
    loop {
        if woken.take() {
            if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
                return v;
            }
        }
        poll_io();
    }
}

pub fn join_all<F: Future>(futures: Vec<F>) -> impl Future<Output = Vec<F::Output>> {
    let n = futures.len();
    JoinAll {
        futs: futures.into_iter().map(|f| Some(Box::pin(f))).collect(),
        out: (0..n).map(|_| None).collect(),
    }
}

struct JoinAll<F: Future> {
    futs: Vec<Option<Pin<Box<F>>>>,
    out: Vec<Option<F::Output>>,
}

pub enum Either<A, B> {
    Left(A),
    Right(B),
}

pub fn race<A: Future, B: Future>(
    a: A,
    b: B,
) -> impl Future<Output = Either<A::Output, B::Output>> {
    Race {
        a: Box::pin(a),
        b: Box::pin(b),
    }
}

struct Race<A: Future, B: Future> {
    a: Pin<Box<A>>,
    b: Pin<Box<B>>,
}

impl<A: Future, B: Future> Future for Race<A, B> {
    type Output = Either<A::Output, B::Output>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Poll::Ready(out) = self.a.as_mut().poll(cx) {
            return Poll::Ready(Either::Left(out));
        }
        if let Poll::Ready(out) = self.b.as_mut().poll(cx) {
            return Poll::Ready(Either::Right(out));
        }
        Poll::Pending
    }
}

pub fn quorum_ok<F, T, E>(futures: Vec<F>, k: usize) -> impl Future<Output = usize>
where
    F: Future<Output = Result<T, E>>,
{
    QuorumOk {
        futs: futures.into_iter().map(|f| Some(Box::pin(f))).collect(),
        ok: 0,
        done: 0,
        k,
    }
}

struct QuorumOk<F> {
    futs: Vec<Option<Pin<Box<F>>>>,
    ok: usize,
    done: usize,
    k: usize,
}

impl<F> Unpin for QuorumOk<F> {}

impl<F, T, E> Future for QuorumOk<F>
where
    F: Future<Output = Result<T, E>>,
{
    type Output = usize;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<usize> {
        let this = self.get_mut();
        let n = this.futs.len();
        for slot in this.futs.iter_mut() {
            if let Some(f) = slot.as_mut() {
                if let Poll::Ready(r) = f.as_mut().poll(cx) {
                    *slot = None;
                    this.done += 1;
                    if r.is_ok() {
                        this.ok += 1;
                    }
                }
            }
        }
        let remaining = n - this.done;
        if this.ok >= this.k || this.ok + remaining < this.k || remaining == 0 {
            Poll::Ready(this.ok)
        } else {
            Poll::Pending
        }
    }
}

pub fn join_until<F, S>(futures: Vec<F>, on_ready: S) -> impl Future<Output = ()>
where
    F: Future,
    S: FnMut(F::Output) -> bool,
{
    JoinUntil {
        futs: futures.into_iter().map(|f| Some(Box::pin(f))).collect(),
        on_ready,
    }
}

struct JoinUntil<F, S> {
    futs: Vec<Option<Pin<Box<F>>>>,
    on_ready: S,
}

impl<F, S> Unpin for JoinUntil<F, S> {}

impl<F, S> Future for JoinUntil<F, S>
where
    F: Future,
    S: FnMut(F::Output) -> bool,
{
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let mut satisfied = false;
        let mut pending = false;
        for slot in this.futs.iter_mut() {
            if let Some(f) = slot.as_mut() {
                match f.as_mut().poll(cx) {
                    Poll::Ready(out) => {
                        *slot = None;
                        satisfied = (this.on_ready)(out);
                    }
                    Poll::Pending => pending = true,
                }
            }
        }
        if satisfied || !pending {
            this.futs.clear();
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

pub fn quorum_collect<F, T, E>(futures: Vec<F>, k: usize) -> impl Future<Output = Vec<T>>
where
    F: Future<Output = Result<T, E>>,
{
    QuorumCollect {
        futs: futures.into_iter().map(|f| Some(Box::pin(f))).collect(),
        got: Vec::new(),
        failed: 0,
        k,
    }
}

struct QuorumCollect<F, T> {
    futs: Vec<Option<Pin<Box<F>>>>,
    got: Vec<T>,
    failed: usize,
    k: usize,
}

impl<F, T> Unpin for QuorumCollect<F, T> {}

impl<F, T, E> Future for QuorumCollect<F, T>
where
    F: Future<Output = Result<T, E>>,
{
    type Output = Vec<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Vec<T>> {
        let this = self.get_mut();
        let n = this.futs.len();
        for slot in this.futs.iter_mut() {
            if let Some(f) = slot.as_mut() {
                if let Poll::Ready(r) = f.as_mut().poll(cx) {
                    *slot = None;
                    match r {
                        Ok(v) => this.got.push(v),
                        Err(_) => this.failed += 1,
                    }
                }
            }
        }
        let collected = this.got.len();
        let remaining = n - collected - this.failed;
        if collected >= this.k || collected + remaining < this.k || remaining == 0 {
            Poll::Ready(std::mem::take(&mut this.got))
        } else {
            Poll::Pending
        }
    }
}

pub fn quorum_collect_reserve<F, T, E>(
    futures: Vec<F>,
    k: usize,
    active: usize,
) -> impl Future<Output = Vec<T>>
where
    F: Future<Output = Result<T, E>>,
{
    let n = futures.len();
    ReserveCollect::<F, T, fn() -> std::future::Pending<()>, std::future::Pending<()>> {
        futs: futures.into_iter().map(|f| Some(Box::pin(f))).collect(),
        active: if n == 0 { 0 } else { active.clamp(1, n) },
        got: Vec::new(),
        failed: 0,
        k,
        hedge: None,
        armed: None,
    }
}

pub fn quorum_collect_reserve_hedged<F, T, E, H, HF>(
    futures: Vec<F>,
    k: usize,
    active: usize,
    hedge: H,
) -> impl Future<Output = Vec<T>>
where
    F: Future<Output = Result<T, E>>,
    H: FnMut() -> HF,
    HF: Future<Output = ()>,
{
    let n = futures.len();
    ReserveCollect {
        futs: futures.into_iter().map(|f| Some(Box::pin(f))).collect(),
        active: if n == 0 { 0 } else { active.clamp(1, n) },
        got: Vec::new(),
        failed: 0,
        k,
        hedge: Some(hedge),
        armed: None,
    }
}

struct ReserveCollect<F, T, H, HF> {
    futs: Vec<Option<Pin<Box<F>>>>,
    active: usize,
    got: Vec<T>,
    failed: usize,
    k: usize,
    hedge: Option<H>,
    armed: Option<Pin<Box<HF>>>,
}

impl<F, T, H, HF> Unpin for ReserveCollect<F, T, H, HF> {}

impl<F, T, E, H, HF> Future for ReserveCollect<F, T, H, HF>
where
    F: Future<Output = Result<T, E>>,
    H: FnMut() -> HF,
    HF: Future<Output = ()>,
{
    type Output = Vec<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Vec<T>> {
        let this = self.get_mut();
        let n = this.futs.len();
        loop {
            let mut pending = 0;
            for slot in this.futs[..this.active].iter_mut() {
                if let Some(f) = slot.as_mut() {
                    match f.as_mut().poll(cx) {
                        Poll::Ready(Ok(v)) => {
                            this.got.push(v);
                            *slot = None;
                        }
                        Poll::Ready(Err(_)) => {
                            this.failed += 1;
                            *slot = None;
                        }
                        Poll::Pending => pending += 1,
                    }
                }
            }
            if this.got.len() >= this.k {
                return Poll::Ready(std::mem::take(&mut this.got));
            }
            if this.got.len() + pending < this.k {
                if this.active < n {
                    this.active += 1;
                    continue;
                }
                return Poll::Ready(std::mem::take(&mut this.got));
            }
            if this.active < n {
                if let Some(hedge) = this.hedge.as_mut() {
                    let armed = this.armed.get_or_insert_with(|| Box::pin(hedge()));
                    if armed.as_mut().poll(cx).is_ready() {
                        this.armed = None;
                        this.active += 1;
                        continue;
                    }
                }
            }
            return Poll::Pending;
        }
    }
}

impl<F: Future> Unpin for JoinAll<F> {}

impl<F: Future> Future for JoinAll<F> {
    type Output = Vec<F::Output>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut pending = false;
        for i in 0..this.futs.len() {
            if let Some(f) = this.futs[i].as_mut() {
                match f.as_mut().poll(cx) {
                    Poll::Ready(v) => {
                        this.out[i] = Some(v);
                        this.futs[i] = None;
                    }
                    Poll::Pending => pending = true,
                }
            }
        }
        if pending {
            Poll::Pending
        } else {
            Poll::Ready(this.out.iter_mut().map(|o| o.take().unwrap()).collect())
        }
    }
}

pub async fn prime<F: Future + ?Sized>(mut fut: Pin<&mut F>) -> Option<F::Output> {
    std::future::poll_fn(move |cx| {
        Poll::Ready(match fut.as_mut().poll(cx) {
            Poll::Ready(v) => Some(v),
            Poll::Pending => None,
        })
    })
    .await
}

pub async fn yield_now() {
    struct YieldOnce(bool);
    impl Future for YieldOnce {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.0 = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
    YieldOnce(false).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::thread;

    type BoxFut = Pin<Box<dyn Future<Output = Result<i32, ()>>>>;

    struct ForeignWake {
        ready: Arc<AtomicBool>,
        sender: Option<Sender<Waker>>,
    }

    impl Future for ForeignWake {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.ready.load(Ordering::Acquire) {
                Poll::Ready(())
            } else {
                if let Some(sender) = self.sender.take() {
                    sender.send(cx.waker().clone()).unwrap();
                }
                Poll::Pending
            }
        }
    }

    fn foreign_wake() -> (ForeignWake, thread::JoinHandle<()>) {
        let ready = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = mpsc::channel();
        let worker_ready = ready.clone();
        let worker = thread::spawn(move || {
            let waker: Waker = receiver.recv().unwrap();
            let cloned = waker.clone();
            worker_ready.store(true, Ordering::Release);
            cloned.wake_by_ref();
            drop(cloned);
            waker.wake();
        });
        (
            ForeignWake {
                ready,
                sender: Some(sender),
            },
            worker,
        )
    }

    #[test]
    fn spawned_task_can_be_woken_from_another_thread() {
        let ex = LocalExecutor::new();
        let completed = Rc::new(Cell::new(false));
        let (wake, worker) = foreign_wake();
        let task_completed = completed.clone();
        ex.spawn(async move {
            wake.await;
            task_completed.set(true);
        });
        ex.run(thread::yield_now);
        worker.join().unwrap();
        assert!(completed.get());
    }

    #[test]
    fn run_until_can_be_woken_from_another_thread() {
        let ex = LocalExecutor::new();
        let (wake, worker) = foreign_wake();
        let out = ex.run_until(
            async move {
                wake.await;
                17
            },
            thread::yield_now,
        );
        worker.join().unwrap();
        assert_eq!(out, 17);
    }

    #[test]
    fn block_on_can_be_woken_from_another_thread() {
        let (wake, worker) = foreign_wake();
        block_on(wake, thread::yield_now);
        worker.join().unwrap();
    }

    struct WakeDuringPoll {
        sender: Option<Sender<Waker>>,
        woke: Receiver<()>,
        first_poll: bool,
    }

    impl Future for WakeDuringPoll {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if !self.first_poll {
                return Poll::Ready(());
            }
            self.sender
                .take()
                .unwrap()
                .send(cx.waker().clone())
                .unwrap();
            self.woke.recv().unwrap();
            self.first_poll = false;
            Poll::Pending
        }
    }

    #[test]
    fn wake_from_another_thread_during_poll_is_not_lost() {
        let (sender, receiver) = mpsc::channel();
        let (woke_sender, woke) = mpsc::channel();
        let worker = thread::spawn(move || {
            let waker: Waker = receiver.recv().unwrap();
            waker.wake_by_ref();
            woke_sender.send(()).unwrap();
        });
        block_on(
            WakeDuringPoll {
                sender: Some(sender),
                woke,
                first_poll: true,
            },
            || {},
        );
        worker.join().unwrap();
    }

    struct CapturedPending {
        sender: Option<Sender<Waker>>,
        dropped: Rc<Cell<bool>>,
    }

    impl Future for CapturedPending {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            self.sender
                .take()
                .unwrap()
                .send(cx.waker().clone())
                .unwrap();
            Poll::Pending
        }
    }

    impl Drop for CapturedPending {
        fn drop(&mut self) {
            self.dropped.set(true);
        }
    }

    #[test]
    fn retained_task_waker_is_safe_after_executor_destruction() {
        let (sender, receiver) = mpsc::channel();
        let dropped = Rc::new(Cell::new(false));
        let ex = LocalExecutor::new();
        ex.spawn(CapturedPending {
            sender: Some(sender),
            dropped: dropped.clone(),
        });
        ex.run_until(async {}, || panic!("foreground future is ready"));
        let waker = receiver.recv().unwrap();
        drop(ex);
        assert!(dropped.get());

        thread::spawn(move || {
            let cloned = waker.clone();
            cloned.wake_by_ref();
            drop(cloned);
            waker.wake();
        })
        .join()
        .unwrap();
    }

    struct CaptureReady {
        sender: Option<Sender<Waker>>,
    }

    impl Future for CaptureReady {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            self.sender
                .take()
                .unwrap()
                .send(cx.waker().clone())
                .unwrap();
            Poll::Ready(())
        }
    }

    struct ReusedSlotProbe {
        stale: Option<Waker>,
        current: Rc<RefCell<Option<Waker>>>,
        gate: Rc<Cell<bool>>,
        premature_polls: Rc<Cell<usize>>,
        first_poll: bool,
    }

    impl Future for ReusedSlotProbe {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.gate.get() {
                return Poll::Ready(());
            }
            if self.first_poll {
                self.first_poll = false;
                *self.current.borrow_mut() = Some(cx.waker().clone());
                self.stale.take().unwrap().wake();
            } else {
                self.premature_polls.set(self.premature_polls.get() + 1);
                *self.current.borrow_mut() = Some(cx.waker().clone());
            }
            Poll::Pending
        }
    }

    #[test]
    fn stale_waker_does_not_notify_a_reused_task_slot() {
        let ex = LocalExecutor::new();
        let (sender, receiver) = mpsc::channel();
        ex.spawn(CaptureReady {
            sender: Some(sender),
        });
        ex.run(|| panic!("first task is ready"));
        let stale = receiver.recv().unwrap();

        let current = Rc::new(RefCell::new(None::<Waker>));
        let gate = Rc::new(Cell::new(false));
        let premature_polls = Rc::new(Cell::new(0));
        ex.spawn(ReusedSlotProbe {
            stale: Some(stale),
            current: current.clone(),
            gate: gate.clone(),
            premature_polls: premature_polls.clone(),
            first_poll: true,
        });
        ex.run(|| {
            gate.set(true);
            current.borrow_mut().take().unwrap().wake();
        });
        assert_eq!(premature_polls.get(), 0);
    }

    #[test]
    fn runs_immediate_tasks() {
        let counter = Rc::new(RefCell::new(0));
        let ex = LocalExecutor::new();
        for _ in 0..10 {
            let c = counter.clone();
            ex.spawn(async move {
                *c.borrow_mut() += 1;
            });
        }
        ex.run(|| panic!("no I/O needed"));
        assert_eq!(*counter.borrow(), 10);
    }

    #[test]
    fn block_on_returns_value() {
        let v = block_on(async { 2 + 3 }, || {});
        assert_eq!(v, 5);
    }

    #[test]
    fn prime_polls_once_and_reports_readiness() {
        use std::cell::Cell;
        use std::rc::Rc;

        let polls = Rc::new(Cell::new(0usize));
        let got = block_on(
            {
                let polls = polls.clone();
                async move {
                    let mut fut = Box::pin(async move {
                        polls.set(polls.get() + 1);
                        42
                    });
                    prime(fut.as_mut()).await
                }
            },
            || {},
        );
        assert_eq!(got, Some(42));
        assert_eq!(polls.get(), 1, "prime polls exactly once");

        let side_effect = Rc::new(Cell::new(false));
        let out = block_on(
            {
                let side_effect = side_effect.clone();
                async move {
                    let mut fut = Box::pin(async move {
                        side_effect.set(true);
                        yield_now().await;
                        7
                    });
                    let primed = prime(fut.as_mut()).await;
                    (primed, fut.await)
                }
            },
            || {},
        );
        assert_eq!(out, (None, 7));
        assert!(side_effect.get(), "first-poll side effect ran during prime");
    }

    #[test]
    fn yield_now_reschedules() {
        let v = block_on(
            async {
                let mut n = 0;
                for _ in 0..5 {
                    yield_now().await;
                    n += 1;
                }
                n
            },
            || {},
        );
        assert_eq!(v, 5);
    }

    #[test]
    fn a_self_yielding_task_does_not_starve_poll_io() {
        const ITERS: usize = 50;
        let ex = LocalExecutor::new();
        let reactor = Rc::new(MockReactor::default());
        let yields = Rc::new(Cell::new(0usize));
        let yields_seen_by_io = Rc::new(Cell::new(usize::MAX));
        {
            let yields = yields.clone();
            ex.spawn(async move {
                for _ in 0..ITERS {
                    yield_now().await;
                    yields.set(yields.get() + 1);
                }
            });
        }
        {
            let reactor = reactor.clone();
            let yields = yields.clone();
            let seen = yields_seen_by_io.clone();
            ex.spawn(async move {
                reactor.submit().await;
                seen.set(yields.get());
            });
        }
        ex.run(|| reactor.complete_all());
        assert_eq!(
            yields.get(),
            ITERS,
            "the yielding task still runs to completion"
        );
        assert!(
            yields_seen_by_io.get() < ITERS,
            "poll_io must interleave (I/O task completed at yield {} of {}); \
             a full-drain drain_ready would only fire poll_io after all {} yields",
            yields_seen_by_io.get(),
            ITERS,
            ITERS,
        );
    }

    #[derive(Default)]
    struct MockReactor {
        pending: RefCell<Vec<(Waker, Rc<RefCell<bool>>)>>,
    }

    struct MockOp {
        done: Rc<RefCell<bool>>,
        reactor: Rc<MockReactor>,
        registered: bool,
    }
    impl Future for MockOp {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if *self.done.borrow() {
                return Poll::Ready(());
            }
            if !self.registered {
                self.registered = true;
                self.reactor
                    .pending
                    .borrow_mut()
                    .push((cx.waker().clone(), self.done.clone()));
            }
            Poll::Pending
        }
    }
    impl MockReactor {
        fn submit(self: &Rc<Self>) -> MockOp {
            MockOp {
                done: Rc::new(RefCell::new(false)),
                reactor: self.clone(),
                registered: false,
            }
        }
        fn complete_all(&self) {
            for (waker, done) in self.pending.borrow_mut().drain(..) {
                *done.borrow_mut() = true;
                waker.wake();
            }
        }
    }

    #[test]
    fn run_until_advances_spawned_tasks_during_a_foreground_op() {
        let ex = LocalExecutor::new();
        let reactor = Rc::new(MockReactor::default());
        let bg_ran = Rc::new(Cell::new(false));
        {
            let reactor = reactor.clone();
            let bg_ran = bg_ran.clone();
            ex.spawn(async move {
                reactor.submit().await;
                bg_ran.set(true);
            });
        }
        let reactor_fg = reactor.clone();
        let out = ex.run_until(
            async move {
                reactor_fg.submit().await;
                42
            },
            || reactor.complete_all(),
        );
        assert_eq!(out, 42);
        assert!(
            bg_ran.get(),
            "the spawned background task must advance during the foreground op"
        );
    }

    #[test]
    fn run_until_returns_while_a_background_task_is_still_parked() {
        let ex = LocalExecutor::new();
        let fg_reactor = Rc::new(MockReactor::default());
        let bg_reactor = Rc::new(MockReactor::default());
        {
            let bg_reactor = bg_reactor.clone();
            ex.spawn(async move {
                bg_reactor.submit().await;
                unreachable!("never completed by poll_io");
            });
        }
        let fg = fg_reactor.clone();
        let out = ex.run_until(
            async move {
                fg.submit().await;
                7
            },
            || fg_reactor.complete_all(),
        );
        assert_eq!(out, 7);
    }

    #[test]
    fn run_until_and_cancel_cancels_a_parked_background_task() {
        let mut ex = LocalExecutor::new();
        let fg_reactor = Rc::new(MockReactor::default());
        let bg_reactor = Rc::new(MockReactor::default());
        {
            let bg_reactor = bg_reactor.clone();
            ex.spawn(async move {
                bg_reactor.submit().await;
                unreachable!("never completed by poll_io");
            });
        }
        let fg = fg_reactor.clone();
        let (out, canceled) = ex.run_until_and_cancel(
            async move {
                fg.submit().await;
                7
            },
            || fg_reactor.complete_all(),
        );
        assert_eq!(out, 7);
        assert_eq!(canceled, 1);
        bg_reactor.complete_all();
        assert_eq!(ex.shared.live(), 0);
    }

    #[test]
    fn run_until_and_cancel_interleaves_poll_io_with_a_self_yielding_task() {
        const ITERS: usize = 50;
        let mut ex = LocalExecutor::new();
        let bg_reactor = Rc::new(MockReactor::default());
        let fg_reactor = Rc::new(MockReactor::default());
        let yields = Rc::new(Cell::new(0usize));
        let yields_seen_by_io = Rc::new(Cell::new(usize::MAX));
        {
            let yields = yields.clone();
            ex.spawn(async move {
                for _ in 0..ITERS {
                    yield_now().await;
                    yields.set(yields.get() + 1);
                }
            });
        }
        {
            let reactor = bg_reactor.clone();
            let yields = yields.clone();
            let seen = yields_seen_by_io.clone();
            ex.spawn(async move {
                reactor.submit().await;
                seen.set(yields.get());
            });
        }
        let fg = fg_reactor.clone();
        let (_, canceled) = ex.run_until_and_cancel(
            async move { fg.submit().await },
            || {
                bg_reactor.complete_all();
                fg_reactor.complete_all();
            },
        );
        assert!(
            yields_seen_by_io.get() < ITERS,
            "poll_io must interleave before the yielding task completes"
        );
        assert_eq!(canceled, 1);
    }

    struct SpawnOnDrop {
        spawner: Spawner,
        spawned_ran: Rc<Cell<bool>>,
    }

    impl Future for SpawnOnDrop {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }

    impl Drop for SpawnOnDrop {
        fn drop(&mut self) {
            let spawned_ran = self.spawned_ran.clone();
            self.spawner.spawn(async move { spawned_ran.set(true) });
        }
    }

    #[test]
    fn cancellation_preserves_tasks_spawned_while_canceled_futures_drop() {
        let mut ex = LocalExecutor::new();
        let spawned_ran = Rc::new(Cell::new(false));
        ex.spawn(SpawnOnDrop {
            spawner: ex.spawner(),
            spawned_ran: spawned_ran.clone(),
        });
        let (_, canceled) = ex.run_until_and_cancel(async {}, || {
            panic!("foreground future is ready")
        });
        assert_eq!(canceled, 1);
        ex.run(|| panic!("drop-spawned task is ready"));
        assert!(spawned_ran.get());
    }

    #[test]
    fn quorum_ok_counts_immediate_results() {
        use std::future::ready;
        let got = block_on(
            quorum_ok(
                vec![ready(Ok::<i32, ()>(1)), ready(Err(())), ready(Ok(3))],
                2,
            ),
            || {},
        );
        assert_eq!(got, 2);
    }

    #[test]
    fn quorum_ok_reports_failure_when_unreachable() {
        use std::future::ready;
        let got = block_on(
            quorum_ok(
                vec![ready(Err::<i32, ()>(())), ready(Err(())), ready(Ok(7))],
                2,
            ),
            || {},
        );
        assert_eq!(got, 1);
    }

    #[test]
    fn quorum_collect_returns_first_k_values() {
        use std::future::ready;
        let got = block_on(
            quorum_collect(
                vec![
                    ready(Ok::<i32, ()>(10)),
                    ready(Err(())),
                    ready(Ok(20)),
                    ready(Ok(30)),
                ],
                2,
            ),
            || {},
        );
        assert!(got.len() >= 2);
        assert!(got.iter().all(|v| [10, 20, 30].contains(v)));
    }

    #[test]
    fn quorum_collect_returns_partial_when_unreachable() {
        use std::future::ready;
        let got = block_on(
            quorum_collect(
                vec![ready(Ok::<i32, ()>(1)), ready(Err(())), ready(Err(()))],
                2,
            ),
            || {},
        );
        assert_eq!(got, vec![1]);
    }

    #[test]
    fn reserve_collect_leaves_reserve_unstarted_when_active_suffices() {
        use std::cell::Cell;
        use std::rc::Rc;
        let started = Rc::new(Cell::new(0u32));
        let mk = |id: i32| {
            let started = started.clone();
            async move {
                started.set(started.get() + 1);
                Ok::<i32, ()>(id)
            }
        };
        let got = block_on(
            quorum_collect_reserve(vec![mk(1), mk(2), mk(3)], 2, 2),
            || {},
        );
        assert_eq!(got.len(), 2);
        assert!(got.iter().all(|v| [1, 2].contains(v)));
        assert_eq!(started.get(), 2, "reserve future must not be started");
    }

    #[test]
    fn reserve_collect_activates_reserve_on_shortfall() {
        use std::future::ready;
        let got = block_on(
            quorum_collect_reserve(
                vec![
                    ready(Err::<i32, ()>(())),
                    ready(Ok::<i32, ()>(2)),
                    ready(Ok::<i32, ()>(3)),
                ],
                2,
                2,
            ),
            || {},
        );
        assert_eq!(got.len(), 2);
        assert!(got.contains(&2) && got.contains(&3));
    }

    #[test]
    fn reserve_collect_returns_partial_when_even_reserve_cannot_reach_k() {
        use std::future::ready;
        let got = block_on(
            quorum_collect_reserve(
                vec![
                    ready(Ok::<i32, ()>(1)),
                    ready(Err::<i32, ()>(())),
                    ready(Err::<i32, ()>(())),
                ],
                2,
                2,
            ),
            || {},
        );
        assert_eq!(got, vec![1]);
    }

    #[test]
    fn reserve_collect_resolves_empty_input_immediately() {
        use std::future::Ready;
        for (k, active) in [(0, 0), (0, 2), (2, 0), (2, 2)] {
            let futs: Vec<Ready<Result<i32, ()>>> = Vec::new();
            let got = block_on(quorum_collect_reserve(futs, k, active), || {});
            assert!(got.is_empty(), "k={k} active={active}");
        }
    }

    #[derive(Default)]
    struct TestClock {
        tick: Cell<u64>,
        wakers: RefCell<Vec<Waker>>,
    }
    impl TestClock {
        fn advance(&self) {
            self.tick.set(self.tick.get() + 1);
            for w in self.wakers.borrow_mut().drain(..) {
                w.wake();
            }
        }
        fn wait_until(self: &Rc<Self>, deadline: u64) -> WaitUntil {
            WaitUntil {
                clock: self.clone(),
                deadline,
            }
        }
    }
    struct WaitUntil {
        clock: Rc<TestClock>,
        deadline: u64,
    }
    impl Future for WaitUntil {
        type Output = ();
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.clock.tick.get() >= self.deadline {
                Poll::Ready(())
            } else {
                self.clock.wakers.borrow_mut().push(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    #[test]
    fn hedged_reserve_activates_after_delay_without_a_failure() {
        const DELAY: u64 = 3;
        let clock = Rc::new(TestClock::default());
        let reactor = Rc::new(MockReactor::default());
        let started = Rc::new(Cell::new(false));

        let r = reactor.clone();
        let slow = async move {
            r.submit().await;
            Ok::<i32, ()>(1)
        };
        let started2 = started.clone();
        let reserve = async move {
            started2.set(true);
            Ok::<i32, ()>(2)
        };

        let hedge_clock = clock.clone();
        let hedge = move || hedge_clock.wait_until(hedge_clock.tick.get() + DELAY);

        let futs: Vec<BoxFut> = vec![Box::pin(slow), Box::pin(reserve)];
        let advance = clock.clone();
        let got = block_on(quorum_collect_reserve_hedged(futs, 1, 1, hedge), || {
            advance.advance()
        });
        assert_eq!(got, vec![2], "reserve completed the read");
        assert!(started.get(), "the hedge started the reserve probe");
    }

    #[test]
    fn hedged_reserve_not_started_when_quorum_reached_first() {
        use std::future::ready;
        let clock = Rc::new(TestClock::default());
        let armed = Rc::new(Cell::new(0u32));
        let started = Rc::new(Cell::new(false));

        let started2 = started.clone();
        let reserve = async move {
            started2.set(true);
            Ok::<i32, ()>(3)
        };
        let futs: Vec<BoxFut> = vec![
            Box::pin(ready(Ok::<i32, ()>(1))),
            Box::pin(ready(Ok::<i32, ()>(2))),
            Box::pin(reserve),
        ];
        let armed2 = armed.clone();
        let hedge_clock = clock.clone();
        let hedge = move || {
            armed2.set(armed2.get() + 1);
            hedge_clock.wait_until(hedge_clock.tick.get() + 1)
        };
        let advance = clock.clone();
        let got = block_on(quorum_collect_reserve_hedged(futs, 2, 2, hedge), || {
            advance.advance()
        });
        assert_eq!(got.len(), 2);
        assert!(got.contains(&1) && got.contains(&2));
        assert_eq!(armed.get(), 0, "hedge never armed once quorum was reached");
        assert!(!started.get(), "reserve never started");
    }

    #[test]
    fn hedged_reserve_still_backfills_failures_immediately() {
        use std::future::ready;
        let clock = Rc::new(TestClock::default());
        let armed = Rc::new(Cell::new(0u32));
        let armed2 = armed.clone();
        let hedge_clock = clock.clone();
        let hedge = move || {
            armed2.set(armed2.get() + 1);
            hedge_clock.wait_until(hedge_clock.tick.get() + 100)
        };
        let futs: Vec<BoxFut> = vec![
            Box::pin(ready(Err::<i32, ()>(()))),
            Box::pin(ready(Ok::<i32, ()>(2))),
            Box::pin(ready(Ok::<i32, ()>(3))),
        ];
        let got = block_on(quorum_collect_reserve_hedged(futs, 2, 2, hedge), || {});
        assert_eq!(got.len(), 2);
        assert!(got.contains(&2) && got.contains(&3));
        assert_eq!(
            armed.get(),
            0,
            "failure back-fill did not go through the hedge"
        );
    }

    #[test]
    fn hedged_reserve_staggers_two_firings() {
        const DELAY: u64 = 3;
        let clock = Rc::new(TestClock::default());
        let reactor = Rc::new(MockReactor::default());
        let start_ticks = Rc::new(RefCell::new(Vec::<u64>::new()));

        let mk_slow = |reactor: Rc<MockReactor>| async move {
            reactor.submit().await;
            Ok::<i32, ()>(0)
        };
        let mk_reserve = |clock: Rc<TestClock>, ticks: Rc<RefCell<Vec<u64>>>, v: i32| async move {
            ticks.borrow_mut().push(clock.tick.get());
            Ok::<i32, ()>(v)
        };
        let futs: Vec<BoxFut> = vec![
            Box::pin(mk_slow(reactor.clone())),
            Box::pin(mk_slow(reactor.clone())),
            Box::pin(mk_reserve(clock.clone(), start_ticks.clone(), 10)),
            Box::pin(mk_reserve(clock.clone(), start_ticks.clone(), 11)),
        ];
        let hedge_clock = clock.clone();
        let hedge = move || hedge_clock.wait_until(hedge_clock.tick.get() + DELAY);
        let advance = clock.clone();
        let got = block_on(quorum_collect_reserve_hedged(futs, 2, 2, hedge), || {
            advance.advance()
        });
        assert_eq!(got.len(), 2);
        assert!(got.contains(&10) && got.contains(&11));
        let ticks = start_ticks.borrow();
        assert_eq!(ticks.len(), 2, "both reserves activated, one per firing");
        assert!(
            ticks[1] >= ticks[0] + DELAY,
            "second firing staggered a further delay after the first ({ticks:?})"
        );
    }

    #[test]
    fn hedged_reserve_never_polls_hedge_without_reserve() {
        use std::future::ready;
        let armed = Rc::new(Cell::new(0u32));
        let armed2 = armed.clone();
        let clock = Rc::new(TestClock::default());
        let hedge_clock = clock.clone();
        let hedge = move || {
            armed2.set(armed2.get() + 1);
            hedge_clock.wait_until(hedge_clock.tick.get() + 1)
        };
        let futs: Vec<BoxFut> = vec![
            Box::pin(ready(Ok::<i32, ()>(1))),
            Box::pin(ready(Ok::<i32, ()>(2))),
        ];
        let got = block_on(quorum_collect_reserve_hedged(futs, 2, 2, hedge), || {});
        assert_eq!(got.len(), 2);
        assert_eq!(armed.get(), 0, "no reserve ⇒ hedge never armed");
    }

    #[test]
    fn hedged_reserve_empty_input_resolves_without_hedge() {
        use std::future::Ready;
        let armed = Rc::new(Cell::new(0u32));
        for (k, active) in [(0usize, 0usize), (0, 2), (2, 0), (2, 2)] {
            let armed2 = armed.clone();
            let clock = Rc::new(TestClock::default());
            let hedge = move || {
                armed2.set(armed2.get() + 1);
                clock.wait_until(1)
            };
            let futs: Vec<Ready<Result<i32, ()>>> = Vec::new();
            let got = block_on(quorum_collect_reserve_hedged(futs, k, active, hedge), || {});
            assert!(got.is_empty(), "k={k} active={active}");
        }
        assert_eq!(armed.get(), 0, "empty input never arms the hedge");
    }

    #[test]
    fn quorum_ok_resolves_at_kth_ack_not_all() {
        let reactor = Rc::new(MockReactor::default());
        let r = reactor.clone();
        let got = block_on(
            async move {
                let futs: Vec<_> = (0..3)
                    .map(|_| {
                        let r = r.clone();
                        async move {
                            r.submit().await;
                            Ok::<_, ()>(())
                        }
                    })
                    .collect();
                quorum_ok(futs, 2).await
            },
            move || reactor.complete_all(),
        );
        assert!(got >= 2, "at least the quorum of acks observed");
    }

    #[test]
    fn join_all_keeps_n_in_flight() {
        let reactor = Rc::new(MockReactor::default());
        let r = reactor.clone();
        let out = block_on(
            async move {
                let futs: Vec<_> = (0..16)
                    .map(|i| {
                        let r = r.clone();
                        async move {
                            r.submit().await;
                            i
                        }
                    })
                    .collect();
                join_all(futs).await
            },
            move || reactor.complete_all(),
        );
        assert_eq!(
            out,
            (0..16).collect::<Vec<_>>(),
            "results in submission order"
        );
    }

    #[test]
    fn many_concurrent_io_futures_complete() {
        let reactor = Rc::new(MockReactor::default());
        let counter = Rc::new(RefCell::new(0));
        let ex = LocalExecutor::new();
        for _ in 0..64 {
            let r = reactor.clone();
            let c = counter.clone();
            ex.spawn(async move {
                r.submit().await;
                *c.borrow_mut() += 1;
            });
        }
        let r = reactor.clone();
        ex.run(move || r.complete_all());
        assert_eq!(*counter.borrow(), 64, "all concurrent I/O futures finished");
    }
}
