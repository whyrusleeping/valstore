use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::os::fd::{OwnedFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

#[derive(Clone, Copy)]
pub enum Interest {
    Read,
    Write,
    ReadWrite,
}

struct Reactor {
    poller: RawFd,
    wakers: HashMap<RawFd, (Waker, Interest)>,
}

thread_local! {
    static REACTOR: RefCell<Option<Reactor>> = const { RefCell::new(None) };
}

fn with_reactor<R>(f: impl FnOnce(&mut Reactor) -> R) -> R {
    REACTOR.with(|cell| {
        let mut slot = cell.borrow_mut();
        let reactor = slot.get_or_insert_with(|| Reactor {
            poller: sys::new_poller(),
            wakers: HashMap::new(),
        });
        f(reactor)
    })
}

pub fn register(fd: RawFd, interest: Interest, waker: Waker) {
    with_reactor(|r| {
        let old = r.wakers.insert(fd, (waker, interest)).map(|(_, old)| old);
        sys::arm(r.poller, fd, old, interest);
    });
}

pub fn deregister(fd: RawFd) {
    let _ = REACTOR.try_with(|cell| {
        if let Some(r) = cell.borrow_mut().as_mut() {
            if let Some((_, old)) = r.wakers.remove(&fd) {
                sys::disarm(r.poller, fd, old);
            }
        }
    });
}

thread_local! {
    static FLUSH_HOOK: std::cell::Cell<Option<fn()>> = const { std::cell::Cell::new(None) };
}

pub fn set_flush_hook(f: fn()) {
    FLUSH_HOOK.with(|h| h.set(Some(f)));
}

fn run_flush_hook() {
    if let Some(f) = FLUSH_HOOK.with(|h| h.get()) {
        f();
    }
}

fn poll_io_with(timeout_ms: i32) {
    let woken: Vec<Waker> = with_reactor(|r| {
        if r.wakers.is_empty() {
            return Vec::new();
        }
        let mut woken = Vec::new();
        sys::wait(r.poller, timeout_ms, |fd| {
            if let Some((w, _)) = r.wakers.get(&fd) {
                woken.push(w.clone());
            }
        });
        woken
    });
    for w in woken {
        w.wake();
    }
}

/// Waits up to 1 ms for socket readiness on this thread and wakes the tasks
/// whose sockets are ready, after flushing queued requests.
///
/// Pass it as the I/O hook of [`vals_rt::block_on`] or a
/// [`vals_rt::LocalExecutor`] that drives a [`NetTransport`](crate::NetTransport);
/// the reactor is per thread, so it must run on the thread that owns the
/// transport.
pub fn poll_io() {
    run_flush_hook();
    poll_io_with(POLL_IO_TIMEOUT_MS);
}

const POLL_IO_TIMEOUT_MS: i32 = 1;

pub fn poll_io_nonblocking() {
    run_flush_hook();
    poll_io_with(0);
}

pub struct Timer {
    fd: OwnedFd,
}

impl Timer {
    pub fn every(period: Duration) -> io::Result<Timer> {
        Ok(Timer {
            fd: sys::timer(period)?,
        })
    }

    pub fn tick(&self) -> Tick<'_> {
        Tick { timer: self }
    }

    pub fn as_raw_fd(&self) -> RawFd {
        use std::os::fd::AsRawFd;
        self.fd.as_raw_fd()
    }

    pub fn drain(&self) {
        use std::os::fd::AsRawFd;
        let _ = sys::take(self.fd.as_raw_fd());
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;
        deregister(self.fd.as_raw_fd());
    }
}

pub struct Tick<'a> {
    timer: &'a Timer,
}

impl Future for Tick<'_> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        use std::os::fd::AsRawFd;
        let fd = self.timer.fd.as_raw_fd();
        if sys::take(fd) {
            Poll::Ready(())
        } else {
            register(fd, Interest::Read, cx.waker().clone());
            Poll::Pending
        }
    }
}

pub struct Notify {
    fd: OwnedFd,
}

impl Default for Notify {
    fn default() -> Self {
        Self::new()
    }
}

impl Notify {
    pub fn new() -> Notify {
        let fd = sys::notifier().unwrap_or_else(|e| panic!("notify fd: {e}"));
        Notify { fd }
    }

    pub fn signal(&self) {
        use std::os::fd::AsRawFd;
        sys::signal(self.fd.as_raw_fd());
    }

    pub fn notified(&self) -> Notified<'_> {
        Notified { notify: self }
    }
}

impl Drop for Notify {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;
        deregister(self.fd.as_raw_fd());
    }
}

pub struct Notified<'a> {
    notify: &'a Notify,
}

impl Future for Notified<'_> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        use std::os::fd::AsRawFd;
        let fd = self.notify.fd.as_raw_fd();
        if sys::take(fd) {
            Poll::Ready(())
        } else {
            register(fd, Interest::Read, cx.waker().clone());
            Poll::Pending
        }
    }
}

#[cfg(target_os = "linux")]
mod sys {
    use super::Interest;
    use std::io;
    use std::os::fd::{FromRawFd, OwnedFd, RawFd};
    use std::time::Duration;

    pub(super) fn new_poller() -> RawFd {
        unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) }
    }

    pub(super) fn arm(epfd: RawFd, fd: RawFd, old: Option<Interest>, interest: Interest) {
        let readiness = match interest {
            Interest::Read => libc::EPOLLIN,
            Interest::Write => libc::EPOLLOUT,
            Interest::ReadWrite => libc::EPOLLIN | libc::EPOLLOUT,
        };
        let mut ev = libc::epoll_event {
            events: (readiness | libc::EPOLLET) as u32,
            u64: fd as u64,
        };
        if old.is_some() {
            unsafe { libc::epoll_ctl(epfd, libc::EPOLL_CTL_MOD, fd, &mut ev) };
            return;
        }
        let rc = unsafe { libc::epoll_ctl(epfd, libc::EPOLL_CTL_ADD, fd, &mut ev) };
        if rc != 0 && io::Error::last_os_error().raw_os_error() == Some(libc::EEXIST) {
            unsafe { libc::epoll_ctl(epfd, libc::EPOLL_CTL_MOD, fd, &mut ev) };
        }
    }

    pub(super) fn disarm(epfd: RawFd, fd: RawFd, _old: Interest) {
        let mut ev = libc::epoll_event { events: 0, u64: 0 };
        unsafe { libc::epoll_ctl(epfd, libc::EPOLL_CTL_DEL, fd, &mut ev) };
    }

    pub(super) fn wait(epfd: RawFd, timeout_ms: i32, mut ready: impl FnMut(RawFd)) {
        let mut events = [libc::epoll_event { events: 0, u64: 0 }; 64];
        let n = unsafe {
            libc::epoll_wait(epfd, events.as_mut_ptr(), events.len() as i32, timeout_ms)
        };
        for ev in events.iter().take(n.max(0) as usize) {
            // SAFETY: `epoll_event` is packed on x86-64, so `u64` may be
            // unaligned; read it through a raw pointer rather than a reference.
            ready(unsafe { std::ptr::addr_of!(ev.u64).read_unaligned() } as RawFd);
        }
    }

    pub(super) fn timer(period: Duration) -> io::Result<OwnedFd> {
        // SAFETY: timerfd_create returns a fresh owned fd (or -1).
        let raw = unsafe {
            libc::timerfd_create(
                libc::CLOCK_MONOTONIC,
                libc::TFD_NONBLOCK | libc::TFD_CLOEXEC,
            )
        };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        let ts = libc::timespec {
            tv_sec: period.as_secs() as libc::time_t,
            tv_nsec: period.subsec_nanos() as libc::c_long,
        };
        let spec = libc::itimerspec {
            it_interval: ts,
            it_value: ts,
        };
        let rc = unsafe { libc::timerfd_settime(raw, 0, &spec, std::ptr::null_mut()) };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(fd)
    }

    pub(super) fn notifier() -> io::Result<OwnedFd> {
        // SAFETY: eventfd returns a fresh owned fd (or -1).
        let raw = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(unsafe { OwnedFd::from_raw_fd(raw) })
    }

    pub(super) fn signal(fd: RawFd) {
        let one = 1u64.to_le_bytes();
        let _ = unsafe { libc::write(fd, one.as_ptr() as *const libc::c_void, 8) };
    }

    pub(super) fn take(fd: RawFd) -> bool {
        let mut buf = [0u8; 8];
        let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, 8) };
        n == 8 || io::Error::last_os_error().kind() != io::ErrorKind::WouldBlock
    }
}

#[cfg(target_os = "macos")]
mod sys {
    use super::Interest;
    use std::io;
    use std::os::fd::{FromRawFd, OwnedFd, RawFd};
    use std::time::Duration;

    fn change(ident: usize, filter: i16, flags: u16, fflags: u32, data: isize) -> libc::kevent {
        libc::kevent {
            ident,
            filter,
            flags,
            fflags,
            data,
            udata: std::ptr::null_mut(),
        }
    }

    fn filters(interest: Interest) -> (bool, bool) {
        match interest {
            Interest::Read => (true, false),
            Interest::Write => (false, true),
            Interest::ReadWrite => (true, true),
        }
    }

    fn apply(kq: RawFd, changes: &[libc::kevent]) {
        if changes.is_empty() {
            return;
        }
        let mut receipts = [change(0, 0, 0, 0, 0); 2];
        unsafe {
            libc::kevent(
                kq,
                changes.as_ptr(),
                changes.len() as i32,
                receipts.as_mut_ptr(),
                receipts.len() as i32,
                std::ptr::null(),
            )
        };
    }

    fn kqueue() -> io::Result<OwnedFd> {
        // SAFETY: kqueue returns a fresh owned fd (or -1).
        let raw = unsafe { libc::kqueue() };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        unsafe { libc::fcntl(raw, libc::F_SETFD, libc::FD_CLOEXEC) };
        Ok(unsafe { OwnedFd::from_raw_fd(raw) })
    }

    pub(super) fn new_poller() -> RawFd {
        use std::os::fd::IntoRawFd;
        kqueue().map(IntoRawFd::into_raw_fd).unwrap_or(-1)
    }

    pub(super) fn arm(kq: RawFd, fd: RawFd, old: Option<Interest>, interest: Interest) {
        let (old_read, old_write) = old.map(filters).unwrap_or((false, false));
        let (read, write) = filters(interest);
        let add = libc::EV_ADD | libc::EV_CLEAR | libc::EV_RECEIPT;
        let delete = libc::EV_DELETE | libc::EV_RECEIPT;
        let mut changes = Vec::with_capacity(2);
        for (filter, want, had) in [
            (libc::EVFILT_READ, read, old_read),
            (libc::EVFILT_WRITE, write, old_write),
        ] {
            if want {
                changes.push(change(fd as usize, filter, add, 0, 0));
            } else if had {
                changes.push(change(fd as usize, filter, delete, 0, 0));
            }
        }
        apply(kq, &changes);
    }

    pub(super) fn disarm(kq: RawFd, fd: RawFd, old: Interest) {
        let (read, write) = filters(old);
        let delete = libc::EV_DELETE | libc::EV_RECEIPT;
        let mut changes = Vec::with_capacity(2);
        if read {
            changes.push(change(fd as usize, libc::EVFILT_READ, delete, 0, 0));
        }
        if write {
            changes.push(change(fd as usize, libc::EVFILT_WRITE, delete, 0, 0));
        }
        apply(kq, &changes);
    }

    fn poll(kq: RawFd, events: &mut [libc::kevent], timeout_ms: i32) -> i32 {
        let ts = libc::timespec {
            tv_sec: (timeout_ms / 1000) as libc::time_t,
            tv_nsec: (timeout_ms % 1000) as libc::c_long * 1_000_000,
        };
        unsafe {
            libc::kevent(
                kq,
                std::ptr::null(),
                0,
                events.as_mut_ptr(),
                events.len() as i32,
                &ts,
            )
        }
    }

    pub(super) fn wait(kq: RawFd, timeout_ms: i32, mut ready: impl FnMut(RawFd)) {
        let mut events = [change(0, 0, 0, 0, 0); 64];
        let n = poll(kq, &mut events, timeout_ms);
        for ev in events.iter().take(n.max(0) as usize) {
            ready(ev.ident as RawFd);
        }
    }

    pub(super) fn timer(period: Duration) -> io::Result<OwnedFd> {
        use std::os::fd::AsRawFd;
        let kq = kqueue()?;
        if period.is_zero() {
            return Ok(kq);
        }
        let nanos = period.as_nanos().min(isize::MAX as u128) as isize;
        let ev = change(0, libc::EVFILT_TIMER, libc::EV_ADD, libc::NOTE_NSECONDS, nanos);
        let rc = unsafe {
            libc::kevent(kq.as_raw_fd(), &ev, 1, std::ptr::null_mut(), 0, std::ptr::null())
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(kq)
    }

    pub(super) fn notifier() -> io::Result<OwnedFd> {
        use std::os::fd::AsRawFd;
        let kq = kqueue()?;
        let ev = change(0, libc::EVFILT_USER, libc::EV_ADD | libc::EV_CLEAR, 0, 0);
        let rc = unsafe {
            libc::kevent(kq.as_raw_fd(), &ev, 1, std::ptr::null_mut(), 0, std::ptr::null())
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(kq)
    }

    pub(super) fn signal(kq: RawFd) {
        let ev = change(0, libc::EVFILT_USER, 0, libc::NOTE_TRIGGER, 0);
        unsafe { libc::kevent(kq, &ev, 1, std::ptr::null_mut(), 0, std::ptr::null()) };
    }

    pub(super) fn take(kq: RawFd) -> bool {
        let mut events = [change(0, 0, 0, 0, 0); 1];
        poll(kq, &mut events, 0) != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn signal_before_await_resolves_immediately() {
        let n = Notify::new();
        n.signal();
        vals_rt::block_on(n.notified(), poll_io_nonblocking);
    }

    #[test]
    fn timer_ticks_repeatedly() {
        let timer = Timer::every(Duration::from_millis(2)).expect("timer");
        let start = std::time::Instant::now();
        vals_rt::block_on(
            async {
                for _ in 0..3 {
                    timer.tick().await;
                }
            },
            poll_io,
        );
        assert!(start.elapsed() >= Duration::from_millis(4));
    }

    #[test]
    fn coalesces_and_then_parks_until_signaled() {
        let n = Notify::new();
        n.signal();
        n.signal();
        n.signal();
        vals_rt::block_on(n.notified(), poll_io_nonblocking);

        let turns = Cell::new(0u32);
        vals_rt::block_on(n.notified(), || {
            turns.set(turns.get() + 1);
            if turns.get() == 3 {
                n.signal();
            }
            poll_io_nonblocking();
        });
        assert!(
            turns.get() >= 3,
            "second notified() parked until the new signal (got {} turns)",
            turns.get()
        );
    }
}
