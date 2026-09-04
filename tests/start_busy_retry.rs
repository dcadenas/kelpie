//! Daemon regression for `agent.start` refused by Herdr.
//!
//! Herdr answers `agent_pane_busy` for a pane whose shell is still sourcing its
//! rc files, a window of a few hundred milliseconds after the pane spawns. The
//! daemon must retry that within the busy budget instead of answering the
//! client from the lease failure, and every refusal must settle the operation
//! in the store rather than leave it `pending` with a `starting` incarnation.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use kelpie::daemon::Daemon;
use kelpie::domain::{InitialMessageIntent, InitialMessageKind, Parent, StartIntent};
use kelpie::herdr::HerdrClient;
use kelpie::slice::Kelpie;
use kelpie::store::Store;
use serde_json::Value;

/// One expected Herdr request and the full response line to answer it with.
type Exchange = (&'static str, Value);

fn ok(result: Value) -> Value {
    Value::Object(serde_json::Map::from_iter([("result".to_owned(), result)]))
}

fn refused(code: &str, message: &str) -> Value {
    serde_json::json!({ "error": { "code": code, "message": message } })
}

fn free_snapshot() -> Value {
    ok(serde_json::json!({
        "type":"session_snapshot",
        "snapshot":{
            "protocol":20,
            "panes":[{"pane_id":"w1:p1","terminal_id":"term-1","cwd":"/tmp/work"}],
            "agents":[]
        }
    }))
}

fn agent(interactive_ready: bool) -> Value {
    serde_json::json!({
        "terminal_id":"term-1","pane_id":"w1:p1","name":"worker",
        "agent":"codex","interactive_ready":interactive_ready,
        "launch_pending":!interactive_ready
    })
}

/// Serve `exchanges` one connection each, asserting the method order, and
/// return the methods seen.
fn spawn_fake_herdr(socket: &Path, exchanges: Vec<Exchange>) -> thread::JoinHandle<Vec<String>> {
    let listener = UnixListener::bind(socket).expect("bind fake Herdr");
    listener.set_nonblocking(true).expect("nonblocking");
    thread::spawn(move || {
        let mut seen = Vec::new();
        let give_up = std::time::Instant::now() + Duration::from_secs(5);
        for (expected, mut response) in exchanges {
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if std::time::Instant::now() >= give_up {
                            // The daemon stopped talking: hand back what was seen.
                            return seen;
                        }
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("accept Herdr request: {error}"),
                }
            };
            stream.set_nonblocking(false).expect("blocking stream");
            let mut line = String::new();
            BufReader::new(stream.try_clone().expect("clone"))
                .read_line(&mut line)
                .expect("read");
            let request: Value = serde_json::from_str(&line).expect("JSON");
            assert_eq!(request["method"], expected, "unexpected request {line}");
            seen.push(expected.to_owned());
            response["id"] = request["id"].clone();
            serde_json::to_writer(&mut stream, &response).expect("write");
            stream.write_all(b"\n").expect("newline");
        }
        seen
    })
}

fn start_request(id: &str) -> Value {
    serde_json::json!({
        "id": id,
        "method": "start",
        "params": StartIntent {
            public_name: "worker".into(),
            logical_agent_id: None,
            parent: Parent::Parentless,
            herdr_session: "test".into(),
            pane_id: "w1:p1".into(),
            expected_terminal_id: "term-1".into(),
            backend_kind: "codex".into(),
            backend_args: vec![],
            initial_message: InitialMessageIntent {
                sender: None,
                kind: InitialMessageKind::Tell,
                body: "initial work".into(),
            },
            working_directory: "/tmp/work".into(),
            idempotency_key: format!("{id}-key"),
            readiness_timeout_ms: 15_000,
            keep_open: true,
            supersedes: None,
            requested_model: None,
            requested_provider: None,
            requested_effort: None,
        }
    })
}

/// Run one start against a daemon backed by `exchanges` and return the client
/// response together with the methods the fake Herdr served.
///
/// The daemon is driven through [`Daemon::poll`], the same parked state machine
/// `kelpied` runs, not `serve_one`, which settles a start inline through the
/// synchronous slice path and would hide a daemon-only regression.
fn run_start(directory: &Path, request_id: &str, exchanges: Vec<Exchange>) -> (Value, Vec<String>) {
    let database = directory.join("kelpie.sqlite3");
    let kelpie_socket = directory.join("kelpie.sock");
    let herdr_socket = directory.join("herdr.sock");
    let herdr = spawn_fake_herdr(&herdr_socket, exchanges);
    let store = Store::open(&database).expect("store");
    let client = HerdrClient::new(&herdr_socket, Duration::from_secs(5));
    let mut daemon = Daemon::bind(&kelpie_socket, Kelpie::new(store, client)).expect("bind");
    let answered = Arc::new(AtomicBool::new(false));
    let server = {
        let answered = Arc::clone(&answered);
        thread::spawn(move || {
            while !answered.load(Ordering::SeqCst) {
                if !daemon.poll().expect("poll") {
                    thread::sleep(Duration::from_millis(10));
                }
            }
        })
    };

    let mut stream = UnixStream::connect(&kelpie_socket).expect("connect kelpie");
    serde_json::to_writer(&stream, &start_request(request_id)).expect("write request");
    stream.write_all(b"\n").expect("newline");
    let mut line = String::new();
    BufReader::new(stream)
        .read_line(&mut line)
        .expect("read response");
    let response: Value = serde_json::from_str(line.trim()).expect("response JSON");
    assert_eq!(response["id"], request_id);
    answered.store(true, Ordering::SeqCst);

    server.join().expect("daemon thread");
    let seen = herdr.join().expect("herdr thread");
    (response, seen)
}

fn operation_row(directory: &Path, operation_id: &str) -> (String, String) {
    let connection = rusqlite::Connection::open(directory.join("kelpie.sqlite3")).expect("open");
    connection
        .query_row(
            "SELECT o.outcome, i.state FROM operations o
             JOIN incarnations i ON i.id = o.target_incarnation_id
             WHERE o.id = ?1",
            [operation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("operation row")
}

#[test]
fn a_busy_pane_is_retried_by_the_daemon_and_the_start_succeeds() {
    let directory = tempfile::tempdir().expect("tempdir");
    let exchanges = vec![
        (
            "ping",
            ok(serde_json::json!({"type":"pong","version":"test","protocol":20})),
        ),
        ("session.snapshot", free_snapshot()),
        // The shell is still initializing: Herdr received the start and refused.
        (
            "agent.start",
            refused(
                "agent_pane_busy",
                "agent target pane w1:p1 is not an available shell",
            ),
        ),
        // Busy retry: re-check the pane is still ours, then start again.
        ("session.snapshot", free_snapshot()),
        (
            "agent.start",
            ok(serde_json::json!({
                "type":"agent_started","agent":agent(false),"argv":["codex"]
            })),
        ),
        (
            "agent.get",
            ok(serde_json::json!({"type":"agent_info","agent":agent(true)})),
        ),
        (
            "agent.prompt",
            ok(serde_json::json!({"type":"agent_prompted","agent":agent(true)})),
        ),
    ];

    let (response, seen) = run_start(directory.path(), "busy-retry-1", exchanges);

    assert!(
        response.get("error").is_none_or(Value::is_null),
        "start must succeed after the busy retry: {response}"
    );
    assert_eq!(response["result"]["runtime_start"]["outcome"], "succeeded");
    assert_eq!(
        seen.iter()
            .filter(|method| *method == "agent.start")
            .count(),
        2,
        "exactly one retry after the busy refusal: {seen:?}"
    );
    let operation_id = response["result"]["runtime_start"]["operation_id"]
        .as_str()
        .expect("operation id");
    assert_eq!(
        operation_row(directory.path(), operation_id),
        ("succeeded".to_owned(), "ready".to_owned())
    );
}

#[test]
fn a_deterministic_refusal_is_answered_once_and_settled_in_the_store() {
    let directory = tempfile::tempdir().expect("tempdir");
    let exchanges = vec![
        (
            "ping",
            ok(serde_json::json!({"type":"pong","version":"test","protocol":20})),
        ),
        ("session.snapshot", free_snapshot()),
        (
            "agent.start",
            refused(
                "agent_name_taken",
                "agent name worker is already used; candidates: none",
            ),
        ),
    ];

    let (response, seen) = run_start(directory.path(), "refused-1", exchanges);

    let message = response["error"]["message"]
        .as_str()
        .unwrap_or_else(|| panic!("refusal must reach the client: {response}"));
    assert!(message.contains("agent_name_taken"), "{message}");
    assert_eq!(
        seen.iter()
            .filter(|method| *method == "agent.start")
            .count(),
        1,
        "a deterministic refusal must not be retried: {seen:?}"
    );

    let connection =
        rusqlite::Connection::open(directory.path().join("kelpie.sqlite3")).expect("open");
    let (outcome, state): (String, String) = connection
        .query_row(
            "SELECT o.outcome, i.state FROM operations o
             JOIN incarnations i ON i.id = o.target_incarnation_id
             WHERE o.kind = 'start'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("start operation row");
    assert_eq!((outcome, state), ("failed".to_owned(), "failed".to_owned()));
}
