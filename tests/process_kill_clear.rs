//! Real-`kelpied` process-kill coverage for standalone clear writes.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
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
const CLEAR_SUBMITTED: &str = "clear_after_submitted_before_write";
const CLEAR_WRITTEN: &str = "clear_after_write_before_response";
const CLEAR_RESPONDED: &str = "clear_after_response_before_commit";

fn intent() -> StartIntent {
    StartIntent {
        public_name: "worker".into(),
        logical_agent_id: None,
        parent: Parent::Parentless,
        herdr_session: "clear-fault-test".into(),
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

fn authoritative_agents() -> Value {
    serde_json::json!([observation()])
}

fn seed_ready_agent(database: &Path) -> DeclaredStart {
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
        .expect("accept seed readiness");
    worker
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

fn prompt_result() -> Value {
    serde_json::json!({"type":"agent_prompted","agent":observation()})
}

fn spawn_clear_herdr(socket: &Path, point: &'static str) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind fake Herdr");
    thread::spawn(move || {
        for (method, result) in [
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            (
                "session.snapshot",
                serde_json::json!({
                    "type":"session_snapshot",
                    "snapshot":{"protocol":20,"panes":[],"agents":authoritative_agents()}
                }),
            ),
            (
                "agent.get",
                serde_json::json!({"type":"agent_info","agent":observation()}),
            ),
        ] {
            let (mut stream, _) = listener.accept().expect("accept startup exchange");
            respond(&mut stream, method, &result);
        }

        let (mut prompt, _) = listener.accept().expect("accept clear prompt");
        if point == CLEAR_SUBMITTED {
            let mut bytes = Vec::new();
            prompt
                .read_to_end(&mut bytes)
                .expect("read until daemon death");
            assert!(bytes.is_empty(), "clear bytes crossed the pre-write point");
            return;
        }

        let mut line = String::new();
        BufReader::new(prompt.try_clone().expect("clone prompt"))
            .read_line(&mut line)
            .expect("read clear request");
        let request: Value = serde_json::from_str(&line).expect("clear request JSON");
        assert_eq!(request["method"], "agent.prompt");
        assert_eq!(request["params"]["target"], "w1:p1");
        assert_eq!(request["params"]["text"], "/clear");
        if point == CLEAR_WRITTEN {
            let mut remainder = Vec::new();
            prompt
                .read_to_end(&mut remainder)
                .expect("wait for daemon death without response");
            assert!(remainder.is_empty());
            return;
        }
        serde_json::to_writer(
            &mut prompt,
            &serde_json::json!({"id":request["id"],"result":prompt_result()}),
        )
        .expect("write clear response");
        prompt.write_all(b"\n").expect("finish clear response");
    })
}

fn spawn_recovery_herdr(socket: &Path) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind recovery Herdr");
    thread::spawn(move || {
        for (method, result) in [
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            (
                "session.snapshot",
                serde_json::json!({
                    "type":"session_snapshot",
                    "snapshot":{"protocol":20,"panes":[],"agents":authoritative_agents()}
                }),
            ),
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

fn send_clear(socket: &Path, worker: DeclaredStart, key: &str) -> thread::JoinHandle<Vec<u8>> {
    let socket = socket.to_path_buf();
    let key = key.to_string();
    thread::spawn(move || {
        let mut stream = UnixStream::connect(socket).expect("connect client");
        serde_json::to_writer(
            &mut stream,
            &serde_json::json!({
                "id":"kill-clear",
                "method":"clear",
                "params":{
                    "recipient":worker.logical_agent_id,
                    "recipient_incarnation":worker.incarnation_id,
                    "idempotency_key":key,
                }
            }),
        )
        .expect("write clear request");
        stream.write_all(b"\n").expect("finish clear request");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .expect("read until daemon death");
        response
    })
}

fn clear_state(database: &Path, key: &str) -> (String, String, i64) {
    Connection::open(database)
        .expect("open database")
        .query_row(
            "SELECT o.outcome, a.phase, a.attempt_number
             FROM operations o JOIN operation_attempts a ON a.operation_id = o.id
             WHERE o.kind = 'clear' AND o.idempotency_key = ?1",
            [key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("clear state")
}

fn run_boundary(point: &'static str) {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let key = format!("fault-clear-{point}");
    let worker = seed_ready_agent(&database);
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let first_herdr = spawn_clear_herdr(&herdr_socket, point);
    let mut first_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},{point}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon");
    let client = send_clear(&kelpie_socket, worker, &key);
    let point_stream = accept_point(&fault_listener, point);
    first_daemon.kill().expect("kill daemon");
    first_daemon.wait().expect("reap daemon");
    drop(point_stream);
    assert!(client.join().expect("client").is_empty());
    first_herdr.join().expect("first Herdr");
    assert_eq!(
        clear_state(&database, &key),
        ("pending".into(), "submitted".into(), 1)
    );

    fs::remove_file(&kelpie_socket).expect("remove Kelpie socket");
    fs::remove_file(&herdr_socket).expect("remove Herdr socket");
    let recovery_herdr = spawn_recovery_herdr(&herdr_socket);
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

    assert_eq!(
        clear_state(&database, &key),
        ("unknown".into(), "unknown".into(), 1)
    );
}

#[test]
fn clear_effect_boundaries_recover_unknown_without_resend() {
    for point in [CLEAR_SUBMITTED, CLEAR_WRITTEN, CLEAR_RESPONDED] {
        run_boundary(point);
    }
}
