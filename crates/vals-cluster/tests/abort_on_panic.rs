#![cfg(target_os = "linux")]

use std::fs::OpenOptions;
use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use vals_cluster::observe;

struct PanickingLogger;

impl log::Log for PanickingLogger {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }

    fn log(&self, _: &log::Record) {
        panic!("deliberate logger panic");
    }

    fn flush(&self) {}
}

struct PanickingPayload;

impl Drop for PanickingPayload {
    fn drop(&mut self) {
        panic!("panic payload must not be dropped before abort");
    }
}

#[test]
fn child_scenario() {
    let Ok(scenario) = std::env::var("VALS_TEST_ABORT_CHILD") else {
        return;
    };
    let limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: limit points to an initialized rlimit for this child process.
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_CORE, &limit) }, 0);
    match scenario.as_str() {
        "panicking_logger" | "logger_panic" => {
            static LOGGER: PanickingLogger = PanickingLogger;
            log::set_logger(&LOGGER).unwrap();
            log::set_max_level(log::LevelFilter::Info);
        }
        "logging_off" => observe::init_stderr_logger_at(log::LevelFilter::Off),
        _ => observe::init_stderr_logger_at(log::LevelFilter::Info),
    }
    let h = observe::spawn_abort_on_panic("vals-shard-test", move || match scenario.as_str() {
        "ordinary_log" => {
            for i in 0..10 {
                log::info!("ordinary shard log {i}");
            }
        }
        "logger_panic" => log::error!("logger panics inside worker"),
        "payload_drop" => std::panic::panic_any(PanickingPayload),
        _ => panic!("deliberate test panic"),
    });
    h.join().unwrap();
}

fn child(scenario: &str, full_stderr: bool) -> Output {
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.arg("child_scenario")
        .arg("--exact")
        .arg("--nocapture")
        .env("VALS_TEST_ABORT_CHILD", scenario)
        .env("RUST_BACKTRACE", "0")
        .stdout(Stdio::null());
    if full_stderr {
        cmd.stderr(OpenOptions::new().write(true).open("/dev/full").unwrap());
    } else {
        cmd.stderr(Stdio::piped());
    }
    let mut child = cmd.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let out = child.wait_with_output().unwrap();
            panic!("child {scenario} stayed alive: {out:?}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn assert_aborted(out: &Output) {
    assert_eq!(out.status.signal(), Some(libc::SIGABRT), "{out:?}");
}

#[test]
fn a_panicking_supervised_thread_aborts_the_process() {
    let out = child("worker_panic", false);
    assert_aborted(&out);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("vals-shard-test"), "{stderr}");
    assert!(stderr.contains("deliberate test panic"), "{stderr}");
}

#[test]
fn ordinary_stderr_logging_survives_enospc() {
    let out = child("ordinary_log", true);
    assert!(out.status.success(), "{out:?}");
}

#[test]
fn ordinary_stderr_logging_preserves_format() {
    let out = child("ordinary_log", false);
    assert!(out.status.success(), "{out:?}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    let lines: Vec<_> = stderr.lines().collect();
    assert_eq!(lines.len(), 10, "{stderr}");
    assert!(lines[0].starts_with('['), "{stderr}");
    assert!(
        lines[0].contains("Z INFO  abort_on_panic] ordinary shard log 0"),
        "{stderr}"
    );
    assert!(lines[9].ends_with("ordinary shard log 9"), "{stderr}");
}

#[test]
fn worker_panic_aborts_with_full_stderr() {
    assert_aborted(&child("worker_panic", true));
}

#[test]
fn worker_panic_bypasses_panicking_logger() {
    assert_aborted(&child("panicking_logger", false));
}

#[test]
fn logger_panic_in_worker_aborts() {
    assert_aborted(&child("logger_panic", false));
}

#[test]
fn worker_panic_aborts_with_logging_off() {
    assert_aborted(&child("logging_off", false));
}

#[test]
fn worker_panic_aborts_before_dropping_payload() {
    let out = child("payload_drop", false);
    assert_aborted(&out);
    assert!(
        !String::from_utf8_lossy(&out.stderr).contains("panic payload must not be dropped"),
        "{out:?}"
    );
}
