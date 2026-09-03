//! `kelpied` must never start beside a daemon that is still serving its
//! socket, and must not be stopped by the socket file a dead one left behind.

use std::os::unix::net::UnixListener;
use std::process::{Command, Output};
use std::thread;

fn run_kelpied(directory: &std::path::Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_kelpied"))
        .arg("--database")
        .arg(directory.join("kelpie.sqlite3"))
        .arg("--socket")
        .arg(directory.join("kelpie.sock"))
        .arg("--herdr-socket")
        .arg(directory.join("absent-herdr.sock"))
        .arg("--herdr-wait-ms")
        .arg("0")
        .output()
        .expect("run kelpied")
}

#[test]
fn refuses_to_start_beside_a_live_daemon() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("kelpie.sock");
    let live = UnixListener::bind(&socket).expect("bind stand-in daemon");
    let stand_in = thread::spawn(move || {
        // Accept the probe and hang up; the real daemon does the same.
        let _ = live.accept();
    });

    let output = run_kelpied(directory.path());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(
        stderr.contains("another kelpied is already serving"),
        "{stderr}"
    );
    assert!(
        !stderr.contains("Herdr"),
        "the refusal must come before any Herdr wait: {stderr}"
    );
    assert!(
        !directory.path().join("kelpie.sqlite3").exists(),
        "the refused daemon must not have opened the database"
    );
    stand_in.join().expect("stand-in daemon");
    assert!(
        socket.exists(),
        "the live daemon's socket must be left alone"
    );
}

#[test]
fn clears_the_socket_a_dead_daemon_left_behind() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("kelpie.sock");
    // Bind and drop: the file stays, nothing listens, exactly as after SIGKILL.
    drop(UnixListener::bind(&socket).expect("bind then abandon"));
    assert!(socket.exists());

    let output = run_kelpied(directory.path());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("already serving"),
        "a stale socket is not a live daemon: {stderr}"
    );
    assert!(
        stderr.contains("Herdr is unavailable"),
        "startup must have proceeded to the Herdr wait: {stderr}"
    );
    assert!(!socket.exists(), "the stale socket file must be removed");
}
