//! Real-`kelpied` process-kill coverage for an initial tell before prompt write.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;

use rusqlite::Connection;
use serde_json::Value;

const DAEMON_BOUND: &str = "daemon_bound";
const INITIAL_SUBMITTED: &str = "initial_message_after_submitted_before_write";
const INITIAL_WRITTEN: &str = "initial_message_after_write_before_response";
const INITIAL_RESPONDED: &str = "initial_message_after_response_before_commit";

fn ready_agent() -> Value {
    serde_json::json!({
        "terminal_id":"term-1",
        "pane_id":"w1:p1",
        "name":"worker",
        "agent":"codex",
        "interactive_ready":true,
        "launch_pending":false
    })
}

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
            (
                // Readiness is polled with agent.get, the reconciling read.
                "agent.get",
                serde_json::json!({"type":"agent_info","agent":ready_agent()}),
            ),
        ];
        for (method, result) in exchanges {
            let (mut stream, _) = listener.accept().expect("accept Herdr exchange");
            respond(&mut stream, method, &result);
        }
        let (mut unwritten_prompt, _) = listener.accept().expect("accept prompt connection");
        let mut bytes = Vec::new();
        unwritten_prompt
            .read_to_end(&mut bytes)
            .expect("read prompt connection to process death");
        assert!(
            bytes.is_empty(),
            "initial agent.prompt bytes crossed the fault point"
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
            (
                // Readiness is polled with agent.get, the reconciling read.
                "agent.get",
                serde_json::json!({"type":"agent_info","agent":ready_agent()}),
            ),
            (
                "agent.prompt",
                serde_json::json!({
                    "type":"agent_prompted",
                    "agent":ready_agent()
                }),
            ),
        ];
        for (method, result) in exchanges {
            let (mut stream, _) = listener.accept().expect("accept Herdr exchange");
            respond(&mut stream, method, &result);
        }
    })
}

fn spawn_withholding_initial_tell_herdr(
    socket: &Path,
    parsed_socket: &Path,
) -> thread::JoinHandle<()> {
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
            (
                // Readiness is polled with agent.get, the reconciling read.
                "agent.get",
                serde_json::json!({"type":"agent_info","agent":ready_agent()}),
            ),
        ];
        for (method, result) in exchanges {
            let (mut stream, _) = listener.accept().expect("accept Herdr exchange");
            respond(&mut stream, method, &result);
        }
        let (prompt_stream, _) = listener.accept().expect("accept initial tell prompt");
        let mut line = String::new();
        BufReader::new(prompt_stream.try_clone().expect("clone prompt stream"))
            .read_line(&mut line)
            .expect("read complete initial tell prompt");
        let request: Value = serde_json::from_str(&line).expect("complete initial tell JSON");
        assert_eq!(request["method"], "agent.prompt");
        assert_eq!(request["params"]["target"], "w1:p1");
        let envelope = request["params"]["text"]
            .as_str()
            .expect("initial tell prompt text");
        assert!(
            envelope.starts_with("<kelpie from=operator msg="),
            "{envelope}"
        );
        assert!(
            envelope.ends_with(">\ninitial work\n</kelpie>"),
            "{envelope}"
        );
        assert!(!envelope.contains("reply-to"));
        UnixStream::connect(parsed_socket)
            .expect("connect parsed signal")
            .write_all(b"initial tell parsed\n")
            .expect("report parsed initial tell");
        let mut remainder = Vec::new();
        prompt_stream
            .try_clone()
            .expect("clone withheld prompt")
            .read_to_end(&mut remainder)
            .expect("wait for daemon death without response");
        assert!(remainder.is_empty());
    })
}

fn spawn_recovery_herdr(socket: &Path) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind recovery fake Herdr");
    thread::spawn(move || {
        for (method, result) in [
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            (
                // Recovery re-proves durable state from the authoritative snapshot.
                "session.snapshot",
                serde_json::json!({
                    "type":"session_snapshot",
                    "snapshot":{"protocol":20,"panes":[],"agents":[ready_agent()]}
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

fn send_launch(socket: &Path) -> thread::JoinHandle<Vec<u8>> {
    let socket = socket.to_path_buf();
    thread::spawn(move || {
        let mut stream = UnixStream::connect(socket).expect("connect Kelpie client");
        serde_json::to_writer(
            &mut stream,
            &serde_json::json!({
                "id":"kill-initial-tell",
                "method":"start",
                "params":{
                    "public_name":"worker",
                    "parent":{"kind":"parentless"},
                    "herdr_session":"initial-fault-test",
                    "pane_id":"w1:p1",
                    "expected_terminal_id":"term-1",
                    "backend_kind":"codex",
                    "backend_args":[],
                    "initial_message":{"sender":null,"kind":"tell","body":"initial work"},
                    "working_directory":"/tmp/work",
                    "idempotency_key":"fault-initial-tell",
                    "readiness_timeout_ms":5000,
                    "keep_open":true
                }
            }),
        )
        .expect("write launch request");
        stream.write_all(b"\n").expect("finish launch request");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .expect("read until daemon death");
        response
    })
}

fn runtime_state(database: &Path) -> (String, String, String) {
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
        .expect("durable runtime state")
}

fn initial_tell_state(database: &Path) -> (String, String, String, String, i64, i64, i64) {
    Connection::open(database)
        .expect("open state database")
        .query_row(
            "SELECT m.id, o.outcome, a.phase, d.outcome,
                    a.attempt_number, d.attempt_number,
                    (SELECT COUNT(*) FROM obligations ob WHERE ob.ask_message_id = m.id)
             FROM operations o
             JOIN operation_attempts a ON a.operation_id = o.id
             JOIN deliveries d ON d.operation_id = o.id
             JOIN messages m ON m.id = d.message_id
             WHERE o.kind = 'prompt' AND m.kind = 'tell'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .expect("durable initial tell state")
}

#[test]
fn kill_before_initial_tell_write_preserves_ready_runtime_and_unknown_delivery() {
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
        &format!("{DAEMON_BOUND},{INITIAL_SUBMITTED}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    let client = send_launch(&kelpie_socket);
    let submitted = accept_point(&fault_listener, INITIAL_SUBMITTED);
    first_daemon.kill().expect("kill first kelpied");
    first_daemon.wait().expect("reap first kelpied");
    drop(submitted);
    assert!(client.join().expect("launch client").is_empty());
    first_herdr.join().expect("first Herdr fixture");
    assert_eq!(
        runtime_state(&database),
        (
            "succeeded".into(),
            "response_committed".into(),
            "ready".into()
        )
    );
    let before_recovery = initial_tell_state(&database);
    assert_eq!(
        (
            before_recovery.1.as_str(),
            before_recovery.2.as_str(),
            before_recovery.3.as_str(),
            before_recovery.4,
            before_recovery.5,
            before_recovery.6,
        ),
        ("pending", "submitted", "submitted", 1, 1, 0)
    );

    fs::remove_file(&kelpie_socket).expect("remove killed daemon socket");
    fs::remove_file(&herdr_socket).expect("remove first Herdr socket");
    let recovery_herdr = spawn_recovery_herdr(&herdr_socket);
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
        runtime_state(&database),
        (
            "succeeded".into(),
            "response_committed".into(),
            "ready".into()
        )
    );
    let after_recovery = initial_tell_state(&database);
    assert_eq!(after_recovery.0, before_recovery.0);
    assert_eq!(
        (
            after_recovery.1.as_str(),
            after_recovery.2.as_str(),
            after_recovery.3.as_str(),
            after_recovery.4,
            after_recovery.5,
            after_recovery.6,
        ),
        ("unknown", "unknown", "unknown", 1, 1, 0)
    );
}

#[test]
fn kill_after_initial_tell_write_preserves_ready_runtime_and_unknown_delivery() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let parsed_socket = directory.path().join("parsed.sock");
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let parsed_listener = UnixListener::bind(&parsed_socket).expect("bind parsed harness");
    let first_herdr = spawn_withholding_initial_tell_herdr(&herdr_socket, &parsed_socket);
    let mut first_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},{INITIAL_WRITTEN}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    let client = send_launch(&kelpie_socket);
    let written = accept_point(&fault_listener, INITIAL_WRITTEN);
    accept_parsed(&parsed_listener, "initial tell parsed");
    first_daemon.kill().expect("kill first kelpied");
    first_daemon.wait().expect("reap first kelpied");
    drop(written);
    assert!(client.join().expect("launch client").is_empty());
    first_herdr.join().expect("withholding Herdr fixture");
    assert_eq!(
        runtime_state(&database),
        (
            "succeeded".into(),
            "response_committed".into(),
            "ready".into()
        )
    );
    let before_recovery = initial_tell_state(&database);
    assert_eq!(
        (
            before_recovery.1.as_str(),
            before_recovery.2.as_str(),
            before_recovery.3.as_str(),
            before_recovery.4,
            before_recovery.5,
            before_recovery.6,
        ),
        ("pending", "submitted", "submitted", 1, 1, 0)
    );

    fs::remove_file(&kelpie_socket).expect("remove killed daemon socket");
    fs::remove_file(&herdr_socket).expect("remove first Herdr socket");
    let recovery_herdr = spawn_recovery_herdr(&herdr_socket);
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
        runtime_state(&database),
        (
            "succeeded".into(),
            "response_committed".into(),
            "ready".into()
        )
    );
    let after_recovery = initial_tell_state(&database);
    assert_eq!(after_recovery.0, before_recovery.0);
    assert_eq!(
        (
            after_recovery.1.as_str(),
            after_recovery.2.as_str(),
            after_recovery.3.as_str(),
            after_recovery.4,
            after_recovery.5,
            after_recovery.6,
        ),
        ("unknown", "unknown", "unknown", 1, 1, 0)
    );
}

#[test]
fn kill_after_initial_tell_acceptance_preserves_ready_runtime_and_unknown_delivery() {
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
        &format!("{DAEMON_BOUND},{INITIAL_RESPONDED}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    let client = send_launch(&kelpie_socket);
    let responded = accept_point(&fault_listener, INITIAL_RESPONDED);
    first_daemon.kill().expect("kill first kelpied");
    first_daemon.wait().expect("reap first kelpied");
    drop(responded);
    assert!(client.join().expect("launch client").is_empty());
    first_herdr.join().expect("responding Herdr fixture");
    assert_eq!(
        runtime_state(&database),
        (
            "succeeded".into(),
            "response_committed".into(),
            "ready".into()
        )
    );
    let before_recovery = initial_tell_state(&database);
    assert_eq!(
        (
            before_recovery.1.as_str(),
            before_recovery.2.as_str(),
            before_recovery.3.as_str(),
            before_recovery.4,
            before_recovery.5,
            before_recovery.6,
        ),
        ("pending", "submitted", "submitted", 1, 1, 0)
    );

    fs::remove_file(&kelpie_socket).expect("remove killed daemon socket");
    fs::remove_file(&herdr_socket).expect("remove first Herdr socket");
    let recovery_herdr = spawn_recovery_herdr(&herdr_socket);
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
        runtime_state(&database),
        (
            "succeeded".into(),
            "response_committed".into(),
            "ready".into()
        )
    );
    let after_recovery = initial_tell_state(&database);
    assert_eq!(after_recovery.0, before_recovery.0);
    assert_eq!(
        (
            after_recovery.1.as_str(),
            after_recovery.2.as_str(),
            after_recovery.3.as_str(),
            after_recovery.4,
            after_recovery.5,
            after_recovery.6,
        ),
        ("unknown", "unknown", "unknown", 1, 1, 0)
    );
}
