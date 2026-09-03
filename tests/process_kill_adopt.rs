//! Real-`kelpied` process-kill coverage for unnamed-pane adoption.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;

use rusqlite::Connection;
use serde_json::Value;

const DAEMON_BOUND: &str = "daemon_bound";
const ADOPT_SUBMITTED: &str = "adopt_rename_after_submitted_before_write";
const ADOPT_WRITTEN: &str = "adopt_rename_after_write_before_response";
const ADOPT_RESPONDED: &str = "adopt_rename_after_response_before_commit";

fn observation(name: Option<&str>) -> Value {
    serde_json::json!({
        "terminal_id":"term-1",
        "pane_id":"w1:p1",
        "name":name,
        "agent":"opencode",
        "interactive_ready":true,
        "launch_pending":false,
        "agent_session":"sess-1"
    })
}

fn snapshot(name: Option<&str>) -> Value {
    serde_json::json!({
        "type":"session_snapshot",
        "snapshot":{
            "protocol":20,
            "panes":[{"pane_id":"w1:p1","terminal_id":"term-1","cwd":"/tmp/worker"}],
            "agents":[observation(name)]
        }
    })
}

fn empty_snapshot() -> Value {
    serde_json::json!({
        "type":"session_snapshot",
        "snapshot":{"protocol":20,"panes":[],"agents":[]}
    })
}

fn respond(stream: &mut UnixStream, expected_method: &str, result: &Value) -> Value {
    let mut line = String::new();
    BufReader::new(stream.try_clone().expect("clone stream"))
        .read_line(&mut line)
        .expect("read Herdr request");
    let request: Value = serde_json::from_str(&line).expect("Herdr request JSON");
    assert_eq!(request["method"], expected_method);
    serde_json::to_writer(
        &mut *stream,
        &serde_json::json!({"id":request["id"],"result":result}),
    )
    .expect("write Herdr response");
    stream.write_all(b"\n").expect("finish response");
    request
}

fn spawn_adopt_herdr(socket: &Path, point: &'static str) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind fake Herdr");
    thread::spawn(move || {
        for (method, result) in [
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            ("session.snapshot", empty_snapshot()),
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            ("session.snapshot", snapshot(None)),
        ] {
            let (mut stream, _) = listener.accept().expect("accept exchange");
            respond(&mut stream, method, &result);
        }

        let (mut rename, _) = listener.accept().expect("accept adopt rename");
        if point == ADOPT_SUBMITTED {
            let mut bytes = Vec::new();
            rename
                .read_to_end(&mut bytes)
                .expect("wait for daemon death");
            assert!(bytes.is_empty(), "rename crossed the pre-write boundary");
            return;
        }

        let mut line = String::new();
        BufReader::new(rename.try_clone().expect("clone rename"))
            .read_line(&mut line)
            .expect("read rename request");
        let request: Value = serde_json::from_str(&line).expect("rename request JSON");
        assert_eq!(request["method"], "agent.rename");
        assert_eq!(request["params"]["target"], "w1:p1");
        assert_eq!(request["params"]["name"], "worker");
        if point == ADOPT_WRITTEN {
            let mut remainder = Vec::new();
            rename
                .read_to_end(&mut remainder)
                .expect("wait for daemon death");
            assert!(remainder.is_empty());
            return;
        }
        serde_json::to_writer(
            &mut rename,
            &serde_json::json!({
                "id":request["id"],
                "result":{"type":"agent_info","agent":observation(Some("worker"))}
            }),
        )
        .expect("write rename response");
        rename.write_all(b"\n").expect("finish rename response");
    })
}

fn spawn_recovery_herdr(socket: &Path, renamed: bool) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind recovery Herdr");
    thread::spawn(move || {
        for (method, result) in [
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            ("session.snapshot", snapshot(renamed.then_some("worker"))),
        ] {
            let (mut stream, _) = listener.accept().expect("accept recovery exchange");
            respond(&mut stream, method, &result);
        }
    })
}

fn spawn_kelpied(
    database: &Path,
    kelpie_socket: &Path,
    herdr_socket: &Path,
    fault_socket: &Path,
    points: &str,
) -> Child {
    Command::new(env!("CARGO_BIN_EXE_kelpied"))
        .arg("--database")
        .arg(database)
        .arg("--socket")
        .arg(kelpie_socket)
        .arg("--herdr-socket")
        .arg(herdr_socket)
        .env("KELPIE_TEST_FAULT_POINTS", points)
        .env("KELPIE_TEST_FAULT_SOCKET", fault_socket)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn kelpied")
}

fn accept_point(listener: &UnixListener, expected: &str) -> UnixStream {
    let (stream, _) = listener.accept().expect("accept fault rendezvous");
    let mut line = String::new();
    BufReader::new(stream.try_clone().expect("clone fault stream"))
        .read_line(&mut line)
        .expect("read fault point");
    assert_eq!(line.trim_end(), expected);
    stream
}

fn send_adopt(socket: &Path, key: &str) -> thread::JoinHandle<Vec<u8>> {
    let socket = socket.to_path_buf();
    let key = key.to_string();
    thread::spawn(move || {
        let mut stream = UnixStream::connect(socket).expect("connect client");
        serde_json::to_writer(
            &mut stream,
            &serde_json::json!({
                "id":"kill-adopt",
                "method":"adopt",
                "params":{
                    "pane_id":"w1:p1",
                    "expected_terminal_id":"term-1",
                    "public_name":"worker",
                    "parent":{"kind":"parentless"},
                    "herdr_session":"default",
                    "backend_args":[],
                    "idempotency_key":key
                }
            }),
        )
        .expect("write adopt request");
        stream.write_all(b"\n").expect("finish adopt request");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .expect("read until daemon death");
        response
    })
}

fn adopt_state(database: &Path, key: &str) -> (String, String, String) {
    Connection::open(database)
        .expect("open database")
        .query_row(
            "SELECT o.outcome, a.phase, i.state
             FROM operations o
             JOIN operation_attempts a ON a.operation_id = o.id
             JOIN incarnations i ON i.id = o.target_incarnation_id
             WHERE o.kind = 'adopt' AND o.idempotency_key = ?1",
            [key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("adopt state")
}

fn run_boundary(point: &'static str) {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let key = format!("fault-adopt-{point}");
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let first_herdr = spawn_adopt_herdr(&herdr_socket, point);
    let mut first_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},{point}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon");
    let client = send_adopt(&kelpie_socket, &key);
    let point_stream = accept_point(&fault_listener, point);
    first_daemon.kill().expect("kill daemon");
    first_daemon.wait().expect("reap daemon");
    drop(point_stream);
    assert!(client.join().expect("client").is_empty());
    first_herdr.join().expect("first Herdr");
    assert_eq!(
        adopt_state(&database, &key),
        ("pending".into(), "submitted".into(), "starting".into())
    );

    fs::remove_file(&kelpie_socket).expect("remove Kelpie socket");
    fs::remove_file(&herdr_socket).expect("remove Herdr socket");
    let renamed = point != ADOPT_SUBMITTED;
    let recovery_herdr = spawn_recovery_herdr(&herdr_socket, renamed);
    let mut recovered = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        DAEMON_BOUND,
    );
    let mut recovered_bound = accept_point(&fault_listener, DAEMON_BOUND);
    recovery_herdr.join().expect("recovery Herdr");
    recovered_bound
        .write_all(b"x")
        .expect("release recovered daemon");
    recovered.kill().expect("kill recovered daemon");
    recovered.wait().expect("reap recovered daemon");

    let expected = if renamed {
        (
            "succeeded".into(),
            "response_committed".into(),
            "ready".into(),
        )
    } else {
        ("unknown".into(), "unknown".into(), "unknown".into())
    };
    assert_eq!(adopt_state(&database, &key), expected);
}

#[test]
fn adopt_effect_boundaries_recover_without_resend() {
    for point in [ADOPT_SUBMITTED, ADOPT_WRITTEN, ADOPT_RESPONDED] {
        run_boundary(point);
    }
}

fn spawn_failing_adopt_herdr(socket: &Path, rejected: bool) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind fake Herdr");
    thread::spawn(move || {
        for (method, result) in [
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            ("session.snapshot", empty_snapshot()),
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            ("session.snapshot", snapshot(None)),
        ] {
            let (mut stream, _) = listener.accept().expect("accept exchange");
            respond(&mut stream, method, &result);
        }
        let (mut rename, _) = listener.accept().expect("accept adopt rename");
        let mut line = String::new();
        BufReader::new(rename.try_clone().expect("clone rename"))
            .read_line(&mut line)
            .expect("read rename request");
        let request: Value = serde_json::from_str(&line).expect("rename request JSON");
        assert_eq!(request["method"], "agent.rename");
        if rejected {
            serde_json::to_writer(
                &mut rename,
                &serde_json::json!({
                    "id":request["id"],
                    "error":{"code":"name_taken","message":"taken"}
                }),
            )
            .expect("write rejection");
            rename.write_all(b"\n").expect("finish rejection");
        }
    })
}

fn spawn_inexact_confirm_herdr(socket: &Path) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind fake Herdr");
    thread::spawn(move || {
        for (method, result) in [
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            ("session.snapshot", empty_snapshot()),
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            ("session.snapshot", snapshot(None)),
            (
                "agent.rename",
                serde_json::json!({
                    "type":"agent_info",
                    "agent":observation(Some("worker"))
                }),
            ),
            ("session.snapshot", snapshot(Some("stranger"))),
        ] {
            let (mut stream, _) = listener.accept().expect("accept exchange");
            respond(&mut stream, method, &result);
        }
    })
}

fn run_adopt_failure(rejected: bool) {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let key = if rejected {
        "adopt-rejected"
    } else {
        "adopt-unknown"
    };
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let herdr = spawn_failing_adopt_herdr(&herdr_socket, rejected);
    let mut daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        DAEMON_BOUND,
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon");
    let response = send_adopt(&kelpie_socket, key).join().expect("client");
    herdr.join().expect("Herdr");
    daemon.kill().expect("kill daemon");
    daemon.wait().expect("reap daemon");
    let response: Value = serde_json::from_slice(&response).expect("client response JSON");
    assert_eq!(
        response["error"]["class"],
        if rejected {
            "rejected"
        } else {
            "unknown_outcome"
        }
    );
    let expected = if rejected {
        ("failed".into(), "rejected".into(), "failed".into())
    } else {
        ("unknown".into(), "unknown".into(), "unknown".into())
    };
    assert_eq!(adopt_state(&database, key), expected);
}

#[test]
fn parked_adopt_persists_rejected_and_unknown_rename_outcomes() {
    run_adopt_failure(true);
    run_adopt_failure(false);
}

#[test]
fn parked_adopt_reports_inexact_confirmation_as_unknown() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let herdr = spawn_inexact_confirm_herdr(&herdr_socket);
    let mut daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        DAEMON_BOUND,
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon");
    let response = send_adopt(&kelpie_socket, "adopt-inexact")
        .join()
        .expect("client");
    herdr.join().expect("Herdr");
    daemon.kill().expect("kill daemon");
    daemon.wait().expect("reap daemon");
    let response: Value = serde_json::from_slice(&response).expect("client response JSON");
    assert_eq!(response["error"]["class"], "unknown_outcome");
    assert_eq!(
        adopt_state(&database, "adopt-inexact"),
        ("unknown".into(), "unknown".into(), "unknown".into())
    );
}
