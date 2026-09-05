//! Real-`kelpied` process-kill coverage for Ready-name projection repair.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;

use kelpie::domain::{InitialMessageIntent, InitialMessageKind, Parent, StartIntent};
use kelpie::herdr::AgentObservation;
use kelpie::store::Store;
use rusqlite::Connection;
use serde_json::Value;

const DAEMON_BOUND: &str = "daemon_bound";
const BEFORE_WRITE: &str = "name_projection_after_intent_before_write";
const AFTER_WRITE: &str = "name_projection_after_write_before_response";
const AFTER_RESPONSE: &str = "name_projection_after_response_before_commit";

fn observation(name: Option<&str>) -> Value {
    serde_json::json!({
        "terminal_id":"term-1","pane_id":"w1:p1","name":name,"agent":"opencode",
        "interactive_ready":true,"launch_pending":false,"agent_session":"sess-1"
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

fn respond(stream: &mut UnixStream, method: &str, result: &Value) -> Value {
    let mut line = String::new();
    BufReader::new(stream.try_clone().expect("clone stream"))
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
    request
}

fn spawn_first_herdr(socket: &Path, point: &'static str) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind first Herdr");
    thread::spawn(move || {
        for (method, result) in [
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            ("session.snapshot", snapshot(Some("worker"))),
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            ("session.snapshot", snapshot(None)),
        ] {
            let (mut stream, _) = listener.accept().expect("accept exchange");
            respond(&mut stream, method, &result);
        }
        let (mut rename, _) = listener.accept().expect("accept rename");
        if point == BEFORE_WRITE {
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
            .expect("read rename");
        let request: Value = serde_json::from_str(&line).expect("rename JSON");
        assert_eq!(request["method"], "agent.rename");
        assert_eq!(request["params"]["name"], "worker");
        if point == AFTER_WRITE {
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
        let mut exchanges = vec![
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            ("session.snapshot", snapshot(renamed.then_some("worker"))),
        ];
        if !renamed {
            exchanges.extend([
                (
                    "agent.rename",
                    serde_json::json!({
                        "type":"agent_info","agent":observation(Some("worker"))
                    }),
                ),
                ("session.snapshot", snapshot(Some("worker"))),
                ("session.snapshot", snapshot(Some("worker"))),
            ]);
        }
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
        .expect("spawn kelpied")
}

fn accept_point(listener: &UnixListener, expected: &str) -> UnixStream {
    let (stream, _) = listener.accept().expect("accept fault point");
    let mut line = String::new();
    BufReader::new(stream.try_clone().expect("clone fault stream"))
        .read_line(&mut line)
        .expect("read fault point");
    assert_eq!(line.trim_end(), expected);
    stream
}

fn seed(database: &Path) {
    let mut store = Store::open(database).expect("store");
    let declared = store
        .declare_start(&StartIntent {
            herdr_session: "default".into(),
            pane_id: "w1:p1".into(),
            expected_terminal_id: "term-1".into(),
            public_name: "worker".into(),
            logical_agent_id: None,
            backend_kind: "opencode".into(),
            backend_args: Vec::new(),
            working_directory: "/tmp/worker".into(),
            requested_model: None,
            requested_provider: None,
            requested_effort: None,
            parent: Parent::Parentless,
            initial_message: InitialMessageIntent {
                sender: None,
                kind: InitialMessageKind::Tell,
                body: "seed".into(),
            },
            idempotency_key: "seed-worker".into(),
            readiness_timeout_ms: 1_000,
            keep_open: true,
            supersedes: None,
        })
        .expect("declare");
    store
        .begin_attempt(
            declared.operation_id,
            declared.incarnation_id,
            "seed-request",
        )
        .expect("attempt");
    store
        .accept_start_ready(
            declared.operation_id,
            declared.incarnation_id,
            &AgentObservation {
                terminal_id: "term-1".into(),
                pane_id: "w1:p1".into(),
                name: Some("worker".into()),
                agent: Some("opencode".into()),
                interactive_ready: true,
                launch_pending: false,
                agent_session: Some(serde_json::json!("sess-1")),
            },
            None,
        )
        .expect("ready");
}

fn pending_projection(database: &Path) -> Option<String> {
    Connection::open(database)
        .expect("database")
        .query_row(
            "SELECT pending_rename_to FROM incarnations WHERE state = 'ready'",
            [],
            |row| row.get(0),
        )
        .expect("projection state")
}

fn send_recover(socket: &Path) -> thread::JoinHandle<Vec<u8>> {
    let socket = socket.to_path_buf();
    thread::spawn(move || {
        let mut stream = UnixStream::connect(socket).expect("connect client");
        stream
            .write_all(b"{\"id\":\"repair\",\"method\":\"recover\",\"params\":{}}\n")
            .expect("send recover");
        let mut response = Vec::new();
        stream.read_to_end(&mut response).expect("read until death");
        response
    })
}

fn run_boundary(point: &'static str) {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    seed(&database);
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let first_herdr = spawn_first_herdr(&herdr_socket, point);
    let mut daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},{point}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon");
    let client = send_recover(&kelpie_socket);
    let point_stream = accept_point(&fault_listener, point);
    daemon.kill().expect("kill daemon");
    daemon.wait().expect("reap daemon");
    drop(point_stream);
    assert!(client.join().expect("client").is_empty());
    first_herdr.join().expect("first Herdr");
    assert_eq!(pending_projection(&database).as_deref(), Some("worker"));

    fs::remove_file(&kelpie_socket).expect("remove Kelpie socket");
    fs::remove_file(&herdr_socket).expect("remove Herdr socket");
    let renamed = point != BEFORE_WRITE;
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
    assert_eq!(pending_projection(&database), None);
}

#[test]
fn name_projection_effect_boundaries_recover_without_blind_resend() {
    for point in [BEFORE_WRITE, AFTER_WRITE, AFTER_RESPONSE] {
        run_boundary(point);
    }
}
