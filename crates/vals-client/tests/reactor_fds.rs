use vals_client::reactor::{poll_io_nonblocking, Notify};

fn open_fds() -> usize {
    let dir = if cfg!(target_os = "linux") { "/proc/self/fd" } else { "/dev/fd" };
    std::fs::read_dir(dir).expect("list open fds").count()
}

fn register_and_wake_once() {
    let n = Notify::new();
    vals_rt::block_on(n.notified(), || {
        n.signal();
        poll_io_nonblocking();
    });
}

#[test]
fn thread_exit_closes_its_reactor_poller() {
    std::thread::spawn(register_and_wake_once).join().unwrap();
    let before = open_fds();
    for _ in 0..32 {
        std::thread::spawn(register_and_wake_once).join().unwrap();
    }
    let after = open_fds();
    assert!(
        after < before + 4,
        "32 reactor thread lifecycles grew open fds from {before} to {after}"
    );
}
