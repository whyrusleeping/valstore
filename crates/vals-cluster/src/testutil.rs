use crate::node::NodeHandle;
use crate::pd::PdDaemon;
use std::cell::Cell;
use std::sync::{Condvar, Mutex, OnceLock};

pub(crate) fn isolated_env_test(settings: &[(&str, &str)]) -> bool {
    let thread = std::thread::current();
    let name = thread.name().expect("named test thread");
    if let Ok(child) = std::env::var("VALS_CLUSTER_ENV_TEST_CHILD") {
        assert_eq!(child, name, "child must run only its selected test");
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--include-ignored", "--nocapture"])
        .env("VALS_CLUSTER_ENV_TEST_CHILD", name)
        .envs(settings.iter().copied())
        .output().expect("spawn isolated test");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success() && stdout.contains("test result: ok. 1 passed;"),
        "isolated test {name} failed: {}\n{stdout}\n{}",
        output.status, String::from_utf8_lossy(&output.stderr));
    true
}

const MAX_LIVE_CLUSTERS: usize = 3;

struct Gate {
    available: Mutex<usize>,
    freed: Condvar,
}

fn gate() -> &'static Gate {
    static GATE: OnceLock<Gate> = OnceLock::new();
    GATE.get_or_init(|| {
        raise_fd_soft_limit();
        Gate {
            available: Mutex::new(MAX_LIVE_CLUSTERS),
            freed: Condvar::new(),
        }
    })
}

fn raise_fd_soft_limit() {
    // SAFETY: `getrlimit`/`setrlimit` over a stack `rlimit`; standard libc use.
    unsafe {
        let mut lim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) != 0 {
            return;
        }
        let target = lim.rlim_max.min(65536);
        if target > lim.rlim_cur {
            lim.rlim_cur = target;
            let _ = libc::setrlimit(libc::RLIMIT_NOFILE, &lim);
        }
    }
}

thread_local! {
    static LIVE_ON_THREAD: Cell<usize> = const { Cell::new(0) };
}

fn enter_cluster() {
    LIVE_ON_THREAD.with(|live| {
        if live.get() == 0 {
            let g = gate();
            let mut avail = g.available.lock().unwrap_or_else(|e| e.into_inner());
            while *avail == 0 {
                avail = g.freed.wait(avail).unwrap_or_else(|e| e.into_inner());
            }
            *avail -= 1;
        }
        live.set(live.get() + 1);
    });
}

fn leave_cluster() {
    LIVE_ON_THREAD.with(|live| {
        let n = live.get() - 1;
        live.set(n);
        if n == 0 {
            let g = gate();
            let mut avail = g.available.lock().unwrap_or_else(|e| e.into_inner());
            *avail += 1;
            g.freed.notify_one();
        }
    });
}

#[must_use = "bind the guard to a name so the node lives to the end of the test"]
pub(crate) struct Running(Option<NodeHandle>);

impl std::ops::Deref for Running {
    type Target = NodeHandle;
    fn deref(&self) -> &NodeHandle {
        self.0.as_ref().expect("node handle present until drop")
    }
}

impl Running {
    pub(crate) fn shutdown(mut self) {
        if let Some(handle) = self.0.take() {
            handle.shutdown();
            leave_cluster();
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        if let Some(h) = self.0.take() {
            h.shutdown();
            leave_cluster();
        }
    }
}

pub(crate) fn running(handle: NodeHandle) -> Running {
    enter_cluster();
    Running(Some(handle))
}

#[must_use = "bind the guard to a name so the daemon lives to the end of the test"]
pub(crate) struct RunningPd(Option<PdDaemon>);

impl std::ops::Deref for RunningPd {
    type Target = PdDaemon;
    fn deref(&self) -> &PdDaemon {
        self.0.as_ref().expect("daemon present until drop")
    }
}

impl Drop for RunningPd {
    fn drop(&mut self) {
        if let Some(pd) = self.0.take() {
            pd.shutdown();
            leave_cluster();
        }
    }
}

pub(crate) fn running_pd(pd: PdDaemon) -> RunningPd {
    enter_cluster();
    RunningPd(Some(pd))
}
