use super::*;
use std::process::{Command, Stdio};
use std::time::Instant;

fn subprocess(mode: &str) -> std::process::Output {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "fail_fast_tests::child", "--nocapture"])
        .env("VALS_FAIL_FAST_CHILD", mode)
        .stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if child.try_wait().unwrap().is_some() {
            let output = child.wait_with_output().unwrap();
            assert_eq!(output.status.code(), Some(if mode == "normal" { 0 } else { 75 }),
                "{mode}: stdout={} stderr={}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
            return output;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!("{mode}: fail-fast subprocess timed out; stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn graceful_fail_fast_exits_75_after_shutdown() {
    let output = subprocess("exit75");
    assert_eq!(output.status.code(), Some(75));
    assert!(String::from_utf8(output.stdout).unwrap().contains("shutdown completed"));
}

#[test]
fn normal_shutdown_exits_zero() {
    let output = subprocess("normal");
    assert_eq!(output.status.code(), Some(0));
    assert!(String::from_utf8(output.stdout).unwrap().contains("shutdown completed"));
}

#[test]
fn watchdog_exits_75_even_with_stderr_locked() {
    let output = subprocess("watchdog");
    assert!(String::from_utf8_lossy(&output.stderr).contains("watchdog deadline exceeded; exiting 75"));
    assert_eq!(output.status.code(), Some(75));
    assert!(!String::from_utf8(output.stdout).unwrap().contains("shutdown completed"));
}

#[test]
fn watchdog_exits_75_even_with_a_full_stderr_pipe() {
    let output = subprocess("watchdog-full-pipe");
    assert_eq!(output.status.code(), Some(75));
    assert!(!String::from_utf8(output.stdout).unwrap().contains("shutdown completed"));
}

#[test]
fn child() {
    let Ok(mode) = std::env::var("VALS_FAIL_FAST_CHILD") else { return; };
    vals_cluster::observe::init_stderr_logger();
    let watchdog = mode.starts_with("watchdog");
    let full_pipe = mode == "watchdog-full-pipe";
    if watchdog {
        let (ready, wait) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let stderr = std::io::stderr();
            let mut lock = stderr.lock();
            ready.send(()).unwrap();
            if full_pipe { let _ = std::io::Write::write_all(&mut lock, &[b'x'; 128 * 1024]); }
            loop { std::thread::park(); }
        });
        wait.recv().unwrap();
    }
    let code = finish_shutdown(mode != "normal", 1,
        if watchdog { Duration::from_millis(50) } else { Duration::from_secs(30) }, || {
            if watchdog { loop { std::thread::park(); } }
            println!("shutdown completed");
        });
    std::process::exit(code as i32);
}
