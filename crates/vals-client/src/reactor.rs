use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::os::fd::{FromRawFd, OwnedFd, RawFd};
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
    epfd: RawFd,
    wakers: HashMap<RawFd, Waker>,
}

thread_local! {
    static REACTOR: RefCell<Option<Reactor>> = const { RefCell::new(None) };
}

fn with_reactor<R>(f: impl FnOnce(&mut Reactor) -> R) -> R {
    REACTOR.with(|cell| {
        let mut slot = cell.borrow_mut();
        let reactor = slot.get_or_insert_with(|| Reactor {
            epfd: unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) },
            wakers: HashMap::new(),
        });
        f(reactor)
    })
}

pub fn register(fd: RawFd, interest: Interest, waker: Waker) {
    with_reactor(|r| {
        let readiness = match interest {
            Interest::Read => libc::EPOLLIN,
            Interest::Write => libc::EPOLLOUT,
            Interest::ReadWrite => libc::EPOLLIN | libc::EPOLLOUT,
        };
        let mut ev = libc::epoll_event {
            events: (readiness | libc::EPOLLET) as u32,
            u64: fd as u64,
        };
        if let Some(slot) = r.wakers.get_mut(&fd) {
            *slot = waker;
            unsafe { libc::epoll_ctl(r.epfd, libc::EPOLL_CTL_MOD, fd, &mut ev) };
            return;
        }
        let rc = unsafe { libc::epoll_ctl(r.epfd, libc::EPOLL_CTL_ADD, fd, &mut ev) };
        if rc != 0 && io::Error::last_os_error().raw_os_error() == Some(libc::EEXIST) {
            unsafe { libc::epoll_ctl(r.epfd, libc::EPOLL_CTL_MOD, fd, &mut ev) };
        }
        r.wakers.insert(fd, waker);
    });
}

pub fn deregister(fd: RawFd) {
    let _ = REACTOR.try_with(|cell| {
        if let Some(r) = cell.borrow_mut().as_mut() {
            if r.wakers.remove(&fd).is_some() {
                let mut ev = libc::epoll_event { events: 0, u64: 0 };
                unsafe { libc::epoll_ctl(r.epfd, libc::EPOLL_CTL_DEL, fd, &mut ev) };
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
        let mut events = [libc::epoll_event { events: 0, u64: 0 }; 64];
        let n = unsafe {
            libc::epoll_wait(r.epfd, events.as_mut_ptr(), events.len() as i32, timeout_ms)
        };
        let mut woken = Vec::new();
        for ev in events.iter().take(n.max(0) as usize) {
            // SAFETY: `epoll_event` is packed on x86-64, so `u64` may be
            // unaligned; read it through a raw pointer rather than a reference.
            let fd = unsafe { std::ptr::addr_of!(ev.u64).read_unaligned() } as RawFd;
            if let Some(w) = r.wakers.get(&fd) {
                woken.push(w.clone());
            }
        }
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
        Ok(Timer { fd })
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
        let mut buf = [0u8; 8];
        let _ = unsafe {
            libc::read(
                self.fd.as_raw_fd(),
                buf.as_mut_ptr() as *mut libc::c_void,
                8,
            )
        };
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
        let mut buf = [0u8; 8];
        let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, 8) };
        if n == 8 {
            Poll::Ready(())
        } else if io::Error::last_os_error().kind() == io::ErrorKind::WouldBlock {
            register(fd, Interest::Read, cx.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(())
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
        // SAFETY: eventfd returns a fresh owned fd (or -1).
        let raw = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        assert!(raw >= 0, "eventfd: {}", io::Error::last_os_error());
        Notify {
            fd: unsafe { OwnedFd::from_raw_fd(raw) },
        }
    }

    pub fn signal(&self) {
        use std::os::fd::AsRawFd;
        let one = 1u64.to_le_bytes();
        let _ = unsafe { libc::write(self.fd.as_raw_fd(), one.as_ptr() as *const libc::c_void, 8) };
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
        let mut buf = [0u8; 8];
        let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, 8) };
        if n == 8 {
            Poll::Ready(())
        } else if io::Error::last_os_error().kind() == io::ErrorKind::WouldBlock {
            register(fd, Interest::Read, cx.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(())
        }
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
