//! Transport failures after a parked mutation write preserve unknown outcomes.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;

use kelpie::domain::{InitialMessageIntent, InitialMessageKind, Parent, StartIntent};
use kelpie::herdr::AgentObservation;
use kelpie::store::{DeclaredStart, Store};
use rusqlite::Connection;
use serde_json::Value;

const DAEMON_BOUND: &str = "daemon_bound";

fn intent() -> StartIntent {
    StartIntent {
        public_name: "worker".into(),
        logical_agent_id: None,
        parent: Parent::Parentless,
        herdr_session: "write-failure-test".into(),
        pane_id: "w1:p1".into(),
        expected_terminal_id: "term-1".into(),
        backend_kind: "opencode".into(),
        backend_args: vec![],
        initial_message: InitialMessageIntent {
            sender: None,
            kind: InitialMessageKind::Tell,
            body: "seed only".into(),
        },
        working_directory: "/tmp/work".into(),
        idempotency_key: "worker-start".into(),
        readiness_timeout_ms: 5_000,
        keep_open: true,
        supersedes: None,
        requested_model: None,
        requested_provider: None,
        requested_effort: None,
    }
}

fn observation() -> AgentObservation {
    AgentObservation {
        terminal_id: "term-1".into(),
        pane_id: "w1:p1".into(),
        name: Some("worker".into()),
        agent: Some("opencode".into()),
        interactive_ready: true,
        launch_pending: false,
        agent_session: Some(Value::String("sess-1".into())),
    }
}

fn snapshot() -> Value {
    serde_json::json!({
        "type":"session_snapshot",
        "snapshot":{
            "protocol":20,
            "panes":[{"pane_id":"w1:p1","terminal_id":"term-1","cwd":"/tmp/work"}],
            "agents":[observation()]
        }
    })
}

fn seed(database: &Path) -> DeclaredStart {
    let mut store = Store::open(database).expect("open seed store");
    let worker = store.declare_start(&intent()).expect("declare worker");
    store
        .begin_attempt(worker.operation_id, worker.incarnation_id, "seed-start")
        .expect("begin seed attempt");
    store
        .accept_start_ready(
            worker.operation_id,
            worker.incarnation_id,
            &observation(),
            None,
        )
        .expect("accept ready");
    worker
}

fn respond(stream: &mut UnixStream, method: &str, result: &Value) {
    let mut line = String::new();
    BufReader::new(stream.try_clone().expect("clone"))
        .read_line(&mut line)
        .expect("read request");
    let request: Value = serde_json::from_str(&line).expect("request JSON");
    assert_eq!(request["method"], method);
    serde_json::to_writer(
        &mut *stream,
        &serde_json::json!({"id":request["id"],"result":result}),
    )
    .expect("write response");
    stream.write_all(b"\n").expect("finish response");
}

fn spawn_herdr(socket: &Path, mutation: &'static str) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind fake Herdr");
    thread::spawn(move || {
        for (method, result) in [
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            ("session.snapshot", snapshot()),
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            ("session.snapshot", snapshot()),
        ] {
            let (mut stream, _) = listener.accept().expect("accept exchange");
            respond(&mut stream, method, &result);
        }
        let (stream, _) = listener.accept().expect("accept mutation");
        let mut line = String::new();
        BufReader::new(stream)
            .read_line(&mut line)
            .expect("read mutation");
        let request: Value = serde_json::from_str(&line).expect("mutation JSON");
        assert_eq!(request["method"], mutation);
        // Dropping after reading the flushed request creates a read-phase failure.
    })
}

fn spawn_kelpied(database: &Path, socket: &Path, herdr: &Path, fault: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_kelpied"))
        .arg("--database")
        .arg(database)
        .arg("--socket")
        .arg(socket)
        .arg("--herdr-socket")
        .arg(herdr)
        .env("KELPIE_TEST_FAULT_POINTS", DAEMON_BOUND)
        .env("KELPIE_TEST_FAULT_SOCKET", fault)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn kelpied")
}

fn accept_bound(listener: &UnixListener) -> UnixStream {
    let (stream, _) = listener.accept().expect("accept fault point");
    let mut line = String::new();
    BufReader::new(stream.try_clone().expect("clone"))
        .read_line(&mut line)
        .expect("read point");
    assert_eq!(line.trim_end(), DAEMON_BOUND);
    stream
}

fn rpc(socket: &Path, request: &Value) -> Value {
    let mut stream = UnixStream::connect(socket).expect("connect client");
    serde_json::to_writer(&mut stream, &request).expect("write request");
    stream.write_all(b"\n").expect("finish request");
    let mut line = String::new();
    BufReader::new(stream)
        .read_line(&mut line)
        .expect("read response");
    serde_json::from_str(&line).expect("response JSON")
}

fn run(mutation: &'static str) -> (tempfile::TempDir, DeclaredStart, Value) {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let worker = seed(&database);
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let herdr = spawn_herdr(&herdr_socket, mutation);
    let mut daemon = spawn_kelpied(&database, &socket, &herdr_socket, &fault_socket);
    let mut bound = accept_bound(&fault_listener);
    bound.write_all(b"x").expect("release daemon");
    let request = if mutation == "agent.rename" {
        serde_json::json!({
            "id":"rename-failure","method":"rename",
            "params":{"agent_id":worker.logical_agent_id,"name":"renamed"}
        })
    } else {
        serde_json::json!({
            "id":"retire-failure","method":"retire",
            "params":{"incarnation_id":worker.incarnation_id,"idempotency_key":"retire-failure","close_pane":true}
        })
    };
    let response = rpc(&socket, &request);
    herdr.join().expect("Herdr");
    daemon.kill().expect("kill daemon");
    daemon.wait().expect("reap daemon");
    (directory, worker, response)
}

#[test]
fn rename_read_failure_is_unknown_and_keeps_the_pending_name() {
    let (directory, worker, response) = run("agent.rename");
    assert_eq!(response["error"]["class"], "unknown_outcome");
    let pending: String = Connection::open(directory.path().join("kelpie.sqlite3"))
        .expect("open database")
        .query_row(
            "SELECT pending_rename_to FROM incarnations WHERE id = ?1",
            [worker.incarnation_id.to_string()],
            |row| row.get(0),
        )
        .expect("pending rename");
    assert_eq!(pending, "renamed");
}

#[test]
fn retire_read_failure_is_unknown_and_keeps_retiring() {
    let (directory, worker, response) = run("pane.close");
    assert_eq!(response["error"]["class"], "unknown_outcome");
    let state: String = Connection::open(directory.path().join("kelpie.sqlite3"))
        .expect("open database")
        .query_row(
            "SELECT state FROM incarnations WHERE id = ?1",
            [worker.incarnation_id.to_string()],
            |row| row.get(0),
        )
        .expect("retire state");
    assert_eq!(state, "retiring");
}
