//! Real-`kelpied` process-kill coverage at one persisted start boundary.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;

use rusqlite::Connection;
use serde_json::Value;

const DAEMON_BOUND: &str = "daemon_bound";
const START_SUBMITTED: &str = "start_after_submitted_before_write";
const START_WRITTEN: &str = "start_after_write_before_response";
const START_RESPONDED: &str = "start_after_response_before_commit";

fn respond(stream: &mut UnixStream, expected_method: &str, result: &Value) {
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
}

fn spawn_first_herdr(socket: &Path) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind first fake Herdr");
    thread::spawn(move || {
        let exchanges = [
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            (
                "session.snapshot",
                serde_json::json!({
                    "type":"session_snapshot",
                    "snapshot":{"protocol":20,"panes":[],"agents":[]}
                }),
            ),
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            (
                "session.snapshot",
                serde_json::json!({
                    "type":"session_snapshot",
                    "snapshot":{
                        "protocol":20,
                        "panes":[{
                            "pane_id":"w1:p1","terminal_id":"term-1","cwd":"/tmp/work"
                        }],
                        "agents":[]
                    }
                }),
            ),
        ];
        for (method, result) in exchanges {
            let (mut stream, _) = listener.accept().expect("accept Herdr exchange");
            respond(&mut stream, method, &result);
        }
        let (mut unwritten_start, _) = listener.accept().expect("accept start connection");
        let mut bytes = Vec::new();
        unwritten_start
            .read_to_end(&mut bytes)
            .expect("read start connection to process death");
        assert!(
            bytes.is_empty(),
            "agent.start bytes crossed the fault point"
        );
    })
}

fn spawn_responding_herdr(socket: &Path) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind responding fake Herdr");
    thread::spawn(move || {
        let exchanges = [
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            (
                "session.snapshot",
                serde_json::json!({
                    "type":"session_snapshot",
                    "snapshot":{"protocol":20,"panes":[],"agents":[]}
                }),
            ),
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            (
                "session.snapshot",
                serde_json::json!({
                    "type":"session_snapshot",
                    "snapshot":{
                        "protocol":20,
                        "panes":[{
                            "pane_id":"w1:p1","terminal_id":"term-1","cwd":"/tmp/work"
                        }],
                        "agents":[]
                    }
                }),
            ),
            (
                "agent.start",
                serde_json::json!({
                    "type":"agent_started",
                    "agent":{
                        "terminal_id":"term-1",
                        "pane_id":"w1:p1",
                        "name":"worker",
                        "agent":"codex",
                        "interactive_ready":false,
                        "launch_pending":true
                    },
                    "argv":["codex"]
                }),
            ),
        ];
        for (method, result) in exchanges {
            let (mut stream, _) = listener.accept().expect("accept Herdr exchange");
            respond(&mut stream, method, &result);
        }
    })
}

fn spawn_withholding_start_herdr(socket: &Path, parsed_socket: &Path) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind withholding fake Herdr");
    let parsed_socket = parsed_socket.to_path_buf();
    thread::spawn(move || {
        let exchanges = [
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            (
                "session.snapshot",
                serde_json::json!({
                    "type":"session_snapshot",
                    "snapshot":{"protocol":20,"panes":[],"agents":[]}
                }),
            ),
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            (
                "session.snapshot",
                serde_json::json!({
                    "type":"session_snapshot",
                    "snapshot":{
                        "protocol":20,
                        "panes":[{
                            "pane_id":"w1:p1","terminal_id":"term-1","cwd":"/tmp/work"
                        }],
                        "agents":[]
                    }
                }),
            ),
        ];
        for (method, result) in exchanges {
            let (mut stream, _) = listener.accept().expect("accept Herdr exchange");
            respond(&mut stream, method, &result);
        }
        let (start_stream, _) = listener.accept().expect("accept start request");
        let mut line = String::new();
        BufReader::new(start_stream.try_clone().expect("clone start stream"))
            .read_line(&mut line)
            .expect("read complete start request");
        let request: Value = serde_json::from_str(&line).expect("complete start request JSON");
        assert_eq!(request["method"], "agent.start");
        assert_eq!(request["params"]["name"], "worker");
        assert_eq!(request["params"]["kind"], "codex");
        assert_eq!(request["params"]["pane_id"], "w1:p1");
        UnixStream::connect(parsed_socket)
            .expect("connect parsed signal")
            .write_all(b"start parsed\n")
            .expect("report parsed start");
        let mut remainder = Vec::new();
        start_stream
            .try_clone()
            .expect("clone withheld start")
            .read_to_end(&mut remainder)
            .expect("wait for daemon death without response");
        assert!(remainder.is_empty());
    })
}

fn spawn_recovery_herdr(socket: &Path, agents: Value) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind recovery fake Herdr");
    thread::spawn(move || {
        let exchanges = [
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            (
                "session.snapshot",
                serde_json::json!({
                    "type":"session_snapshot",
                    "snapshot":{"protocol":20,"panes":[],"agents":agents}
                }),
            ),
        ];
        for (method, result) in exchanges {
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
        .expect("spawn real kelpied")
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

fn accept_parsed(listener: &UnixListener, expected: &str) {
    let (stream, _) = listener.accept().expect("accept parsed signal");
    let mut line = String::new();
    BufReader::new(stream)
        .read_line(&mut line)
        .expect("read parsed signal");
    assert_eq!(line.trim_end(), expected);
}

fn send_start(socket: &Path) -> thread::JoinHandle<Vec<u8>> {
    let socket = socket.to_path_buf();
    thread::spawn(move || {
        let mut stream = UnixStream::connect(socket).expect("connect Kelpie client");
        serde_json::to_writer(
            &mut stream,
            &serde_json::json!({
                "id":"kill-start",
                "method":"start",
                "params":{
                    "public_name":"worker",
                    "parent":{"kind":"parentless"},
                    "herdr_session":"fault-test",
                    "pane_id":"w1:p1",
                    "expected_terminal_id":"term-1",
                    "backend_kind":"codex",
                    "backend_args":[],
                    "initial_message":{"sender":null,"kind":"tell","body":"work"},
                    "working_directory":"/tmp/work",
                    "idempotency_key":"fault-start",
                    "readiness_timeout_ms":5000,
                    "keep_open":true
                }
            }),
        )
        .expect("write start request");
        stream.write_all(b"\n").expect("finish start request");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .expect("read until daemon death");
        response
    })
}

fn durable_start_state(database: &Path) -> (String, String, String) {
    Connection::open(database)
        .expect("open state database")
        .query_row(
            "SELECT o.outcome, a.phase, i.state
             FROM operations o
             JOIN operation_attempts a ON a.operation_id = o.id
             JOIN incarnations i ON i.id = o.target_incarnation_id
             WHERE o.kind = 'start'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("durable start state")
}

#[test]
fn kill_after_start_submitted_recovers_unknown_without_resend() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let first_herdr = spawn_first_herdr(&herdr_socket);
    let mut first_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},{START_SUBMITTED}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    let client = send_start(&kelpie_socket);
    let submitted = accept_point(&fault_listener, START_SUBMITTED);
    first_daemon.kill().expect("kill first kelpied");
    first_daemon.wait().expect("reap first kelpied");
    drop(submitted);
    assert!(client.join().expect("start client").is_empty());
    first_herdr.join().expect("first Herdr fixture");
    assert_eq!(
        durable_start_state(&database),
        ("pending".into(), "submitted".into(), "starting".into())
    );

    fs::remove_file(&kelpie_socket).expect("remove killed daemon socket");
    fs::remove_file(&herdr_socket).expect("remove first Herdr socket");
    let recovery_herdr = spawn_recovery_herdr(&herdr_socket, serde_json::json!([]));
    let mut recovered_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        DAEMON_BOUND,
    );
    let mut recovered_bound = accept_point(&fault_listener, DAEMON_BOUND);
    recovered_bound
        .write_all(b"x")
        .expect("release recovered daemon");
    recovery_herdr.join().expect("recovery Herdr fixture");
    recovered_daemon.kill().expect("kill recovered kelpied");
    recovered_daemon.wait().expect("reap recovered kelpied");
    assert_eq!(
        durable_start_state(&database),
        ("unknown".into(), "unknown".into(), "unknown".into())
    );
}

#[test]
fn kill_after_start_write_recovers_unknown_without_resend() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let parsed_socket = directory.path().join("parsed.sock");
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let parsed_listener = UnixListener::bind(&parsed_socket).expect("bind parsed harness");
    let first_herdr = spawn_withholding_start_herdr(&herdr_socket, &parsed_socket);
    let mut first_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},{START_WRITTEN}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    let client = send_start(&kelpie_socket);
    let written = accept_point(&fault_listener, START_WRITTEN);
    accept_parsed(&parsed_listener, "start parsed");
    first_daemon.kill().expect("kill first kelpied");
    first_daemon.wait().expect("reap first kelpied");
    drop(written);
    assert!(client.join().expect("start client").is_empty());
    first_herdr.join().expect("withholding Herdr fixture");
    assert_eq!(
        durable_start_state(&database),
        ("pending".into(), "submitted".into(), "starting".into())
    );

    fs::remove_file(&kelpie_socket).expect("remove killed daemon socket");
    fs::remove_file(&herdr_socket).expect("remove first Herdr socket");
    let recovery_herdr = spawn_recovery_herdr(&herdr_socket, serde_json::json!([]));
    let mut recovered_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        DAEMON_BOUND,
    );
    let recovered_bound = accept_point(&fault_listener, DAEMON_BOUND);
    recovery_herdr.join().expect("recovery Herdr fixture");
    recovered_daemon.kill().expect("kill recovered kelpied");
    recovered_daemon.wait().expect("reap recovered kelpied");
    drop(recovered_bound);
    assert_eq!(
        durable_start_state(&database),
        ("unknown".into(), "unknown".into(), "unknown".into())
    );
}

fn run_response_before_commit_case(agents: Value, expected_state: (&str, &str, &str)) {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let first_herdr = spawn_responding_herdr(&herdr_socket);
    let mut first_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},{START_RESPONDED}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    let client = send_start(&kelpie_socket);
    let responded = accept_point(&fault_listener, START_RESPONDED);
    first_daemon.kill().expect("kill first kelpied");
    first_daemon.wait().expect("reap first kelpied");
    drop(responded);
    assert!(client.join().expect("start client").is_empty());
    first_herdr.join().expect("responding Herdr fixture");
    assert_eq!(
        durable_start_state(&database),
        ("pending".into(), "submitted".into(), "starting".into())
    );

    fs::remove_file(&kelpie_socket).expect("remove killed daemon socket");
    fs::remove_file(&herdr_socket).expect("remove first Herdr socket");
    let recovery_herdr = spawn_recovery_herdr(&herdr_socket, agents);
    let mut recovered_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        DAEMON_BOUND,
    );
    let mut recovered_bound = accept_point(&fault_listener, DAEMON_BOUND);
    recovered_bound
        .write_all(b"x")
        .expect("release recovered daemon");
    recovery_herdr.join().expect("recovery Herdr fixture");
    recovered_daemon.kill().expect("kill recovered kelpied");
    recovered_daemon.wait().expect("reap recovered kelpied");
    assert_eq!(
        durable_start_state(&database),
        (
            expected_state.0.into(),
            expected_state.1.into(),
            expected_state.2.into()
        )
    );
}

#[test]
fn kill_after_start_response_reconciles_exact_ready_without_resend() {
    run_response_before_commit_case(
        serde_json::json!([{
            "terminal_id":"term-1",
            "pane_id":"w1:p1",
            "name":"worker",
            "agent":"codex",
            "interactive_ready":true,
            "launch_pending":false
        }]),
        ("succeeded", "response_committed", "ready"),
    );
}

#[test]
fn kill_after_start_response_preserves_unknown_when_snapshot_is_indecisive() {
    run_response_before_commit_case(
        serde_json::json!([{
            "terminal_id":"term-1",
            "pane_id":"w1:p1",
            "name":"worker",
            "agent":"codex",
            "interactive_ready":false,
            "launch_pending":true
        }]),
        ("unknown", "unknown", "unknown"),
    );
}
