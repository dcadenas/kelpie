//! Real-`kelpied` process-kill coverage for cancellation delivery boundaries.
//!
//! The obligation settles `cancelled` before any Herdr write (durable intent
//! precedes the effect), so killing the daemon at the submitted boundary must
//! leave the cancellation standing while the response write stays provably
//! absent — and recovery must mark the attempt unknown without resending it.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;

use kelpie::domain::{
    InitialMessageIntent, InitialMessageKind, MessageId, ObligationState, Parent, StartIntent,
};
use kelpie::herdr::AgentObservation;
use kelpie::store::{DeclaredStart, Store};
use rusqlite::Connection;
use serde_json::Value;

const DAEMON_BOUND: &str = "daemon_bound";
const CANCEL_SUBMITTED: &str = "cancellation_after_submitted_before_write";

fn intent(name: &str, pane: &str, terminal: &str, key: &str) -> StartIntent {
    StartIntent {
        public_name: name.into(),
        logical_agent_id: None,
        parent: Parent::Parentless,
        herdr_session: "cancel-fault-test".into(),
        pane_id: pane.into(),
        expected_terminal_id: terminal.into(),
        backend_kind: "codex".into(),
        backend_args: vec![],
        initial_message: InitialMessageIntent {
            sender: None,
            kind: InitialMessageKind::Tell,
            body: "seed only".into(),
        },
        working_directory: "/tmp/work".into(),
        idempotency_key: key.into(),
        readiness_timeout_ms: 5_000,
        keep_open: true,
        supersedes: None,
        requested_model: None,
        requested_provider: None,
        requested_effort: None,
    }
}

fn observation(name: &str, pane: &str, terminal: &str) -> AgentObservation {
    AgentObservation {
        terminal_id: terminal.into(),
        pane_id: pane.into(),
        name: Some(name.into()),
        agent: Some("codex".into()),
        interactive_ready: true,
        launch_pending: false,
        agent_session: None,
    }
}

fn authoritative_agents() -> Value {
    serde_json::json!([
        observation("waiting", "w1:p1", "term-waiting"),
        observation("owing", "w1:p2", "term-owing")
    ])
}

fn seed_open_ask(database: &Path) -> (DeclaredStart, DeclaredStart, MessageId) {
    let mut store = Store::open(database).expect("open seed store");
    let waiting = store
        .declare_start(&intent("waiting", "w1:p1", "term-waiting", "waiting-start"))
        .expect("declare waiting");
    let owing = store
        .declare_start(&intent("owing", "w1:p2", "term-owing", "owing-start"))
        .expect("declare owing");
    for (declared, agent) in [
        (waiting, observation("waiting", "w1:p1", "term-waiting")),
        (owing, observation("owing", "w1:p2", "term-owing")),
    ] {
        store
            .begin_attempt(declared.operation_id, declared.incarnation_id, "seed-start")
            .expect("begin seed attempt");
        store
            .accept_start_ready(declared.operation_id, declared.incarnation_id, &agent, None)
            .expect("accept seed readiness");
    }
    let ask = store
        .create_ask(
            waiting.logical_agent_id,
            owing.logical_agent_id,
            owing.incarnation_id,
            "need a final reply",
            "seed-cancel-ask",
        )
        .expect("seed ask");
    (waiting, owing, ask.message_id)
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

/// Serves only the startup exchange, then asserts the cancellation prompt
/// never crossed: the daemon dies at the fault point before writing.
fn spawn_first_herdr(socket: &Path) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind first fake Herdr");
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
            let (mut stream, _) = listener.accept().expect("accept startup exchange");
            respond(&mut stream, method, &result);
        }
        let (mut unwritten_prompt, _) = listener.accept().expect("accept prompt connection");
        let mut bytes = Vec::new();
        unwritten_prompt
            .read_to_end(&mut bytes)
            .expect("read prompt connection to process death");
        assert!(
            bytes.is_empty(),
            "agent.prompt bytes crossed the fault point"
        );
    })
}

/// Serves the startup exchange, receives the cancellation prompt, reports that
/// the bytes and the envelope crossed, and withholds the response.
fn spawn_withholding_cancellation_herdr(
    socket: &Path,
    parsed_socket: &Path,
) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind withholding fake Herdr");
    let parsed_socket = parsed_socket.to_path_buf();
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
            let (mut stream, _) = listener.accept().expect("accept startup exchange");
            respond(&mut stream, method, &result);
        }
        let (prompt_stream, _) = listener.accept().expect("accept cancellation prompt");
        let mut line = String::new();
        BufReader::new(prompt_stream.try_clone().expect("clone prompt stream"))
            .read_line(&mut line)
            .expect("read complete cancellation prompt");
        let request: Value =
            serde_json::from_str(&line).expect("complete cancellation prompt JSON");
        assert_eq!(request["method"], "agent.prompt");
        assert_eq!(request["params"]["target"], "w1:p1");
        let envelope = request["params"]["text"]
            .as_str()
            .expect("cancellation text");
        assert!(envelope.contains("<kelpie-system cancellation"));
        UnixStream::connect(parsed_socket)
            .expect("connect parsed signal")
            .write_all(b"cancellation parsed\n")
            .expect("report parsed cancellation");
        let mut remainder = Vec::new();
        prompt_stream
            .try_clone()
            .expect("clone withheld prompt")
            .read_to_end(&mut remainder)
            .expect("wait for daemon death without response");
        assert!(remainder.is_empty());
    })
}

/// Serves the startup exchange and accepts the cancellation prompt, reporting
/// `agent_prompted` so the daemon reaches the response-before-commit boundary.
fn spawn_responding_cancellation_herdr(socket: &Path) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind responding fake Herdr");
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
                "agent.prompt",
                serde_json::json!({
                    "type":"agent_prompted",
                    "agent":observation("waiting", "w1:p1", "term-waiting")
                }),
            ),
        ] {
            let (mut stream, _) = listener.accept().expect("accept Herdr exchange");
            respond(&mut stream, method, &result);
        }
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
        // Serves startup only. The no-resend proof is durable: recovery must
        // mark the attempt unknown and leave the single delivery row pending,
        // asserted in the main thread after recovery.
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

fn send_cancel(
    socket: &Path,
    requester: &str,
    ask_message_id: MessageId,
) -> thread::JoinHandle<()> {
    let socket = socket.to_path_buf();
    let requester = requester.to_string();
    let ask = ask_message_id.to_string();
    thread::spawn(move || {
        let mut stream = UnixStream::connect(socket).expect("connect Kelpie client");
        serde_json::to_writer(
            &mut stream,
            &serde_json::json!({
                "id":"kill-cancel",
                "method":"cancel",
                "params":{
                    "requester_agent_id":requester,
                    "ask_message_id":ask,
                    "reason":"superseded by a fresh round"
                }
            }),
        )
        .expect("write cancel request");
        stream.write_all(b"\n").expect("finish cancel request");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .expect("read until daemon death");
    })
}

#[test]
fn kill_after_cancellation_submitted_settles_obligation_without_resend() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (waiting, _owing, ask_message_id) = seed_open_ask(&database);
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let first_herdr = spawn_first_herdr(&herdr_socket);
    let mut first_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},{CANCEL_SUBMITTED}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    let client = send_cancel(
        &kelpie_socket,
        &waiting.logical_agent_id.to_string(),
        ask_message_id,
    );
    let submitted = accept_point(&fault_listener, CANCEL_SUBMITTED);
    first_daemon.kill().expect("kill first kelpied");
    first_daemon.wait().expect("reap first kelpied");
    drop(submitted);
    client.join().expect("cancel client");
    first_herdr.join().expect("first Herdr fixture");

    // The semantic act is durable: cancelled before any Herdr write, with the
    // Kelpie-authored response recorded and the delivery still pending.
    let connection = Connection::open(&database).expect("open state database");
    let (obligation_state, message_kind, sender, delivery_outcome, operation_outcome): (
        String,
        String,
        Option<String>,
        String,
        String,
    ) = connection
        .query_row(
            "SELECT ob.state, m.kind, m.sender_agent_id, d.outcome, o.outcome
             FROM obligations ob
             JOIN messages m ON m.recipient_agent_id = ob.waiting_agent_id
              AND m.kind = 'cancellation'
             JOIN deliveries d ON d.message_id = m.id
             JOIN operations o ON o.id = d.operation_id
             WHERE ob.ask_message_id = ?1",
            [ask_message_id.to_string()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .expect("durable cancellation state");
    drop(connection);
    assert_eq!(obligation_state, "cancelled");
    assert_eq!(message_kind, "cancellation");
    assert_eq!(sender, None, "the response is attributed to nobody");
    // mark_submitted ran before the kill: the delivery shows submitted while
    // the operation has no terminal outcome yet.
    assert_eq!(delivery_outcome, "submitted");
    assert_eq!(operation_outcome, "pending");

    // Recovery marks the submitted attempt unknown and never resends: the
    // recovery Herdr serves startup only, and the obligation stays cancelled.
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
    let mut recovered_bound = accept_point(&fault_listener, DAEMON_BOUND);
    recovery_herdr.join().expect("recovery Herdr fixture");
    recovered_bound
        .write_all(b"x")
        .expect("release recovered daemon");
    let (operation_outcome, delivery_outcome): (String, String) = Connection::open(&database)
        .expect("open state database")
        .query_row(
            "SELECT o.outcome, d.outcome
             FROM operations o
             JOIN deliveries d ON d.operation_id = o.id
             WHERE o.kind = 'prompt' AND o.intent_json LIKE '%\"audience\":\"waiting\"%'
             ORDER BY o.created_at_ms DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("recovered cancellation delivery");
    assert_eq!(operation_outcome, "unknown");
    assert_eq!(delivery_outcome, "unknown");
    assert_eq!(
        Store::open(&database)
            .expect("reopen")
            .obligation_state(ask_message_id)
            .expect("state"),
        ObligationState::Cancelled
    );
    recovered_daemon.kill().expect("stop recovered kelpied");
    recovered_daemon.wait().expect("reap recovered kelpied");
}

fn accept_parsed(listener: &UnixListener, expected: &str) {
    let (stream, _) = listener.accept().expect("accept parsed signal");
    let mut line = String::new();
    BufReader::new(stream)
        .read_line(&mut line)
        .expect("read parsed signal");
    assert_eq!(line.trim_end(), expected);
}

fn recover_marking_unknown(
    database: &Path,
    kelpie_socket: &Path,
    herdr_socket: &Path,
    fault_socket: &Path,
    fault_listener: &UnixListener,
    ask_message_id: MessageId,
) {
    fs::remove_file(kelpie_socket).expect("remove killed daemon socket");
    fs::remove_file(herdr_socket).expect("remove first Herdr socket");
    let recovery_herdr = spawn_recovery_herdr(herdr_socket);
    let mut recovered_daemon = spawn_kelpied(
        database,
        kelpie_socket,
        herdr_socket,
        fault_socket,
        DAEMON_BOUND,
    );
    let mut recovered_bound = accept_point(fault_listener, DAEMON_BOUND);
    recovery_herdr.join().expect("recovery Herdr fixture");
    recovered_bound
        .write_all(b"x")
        .expect("release recovered daemon");
    let (operation_outcome, delivery_outcome): (String, String) = Connection::open(database)
        .expect("open state database")
        .query_row(
            "SELECT o.outcome, d.outcome
             FROM operations o
             JOIN deliveries d ON d.operation_id = o.id
             WHERE o.kind = 'prompt' AND o.intent_json LIKE '%\"audience\":\"waiting\"%'
             ORDER BY o.created_at_ms DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("recovered cancellation delivery");
    assert_eq!(operation_outcome, "unknown");
    assert_eq!(delivery_outcome, "unknown");
    assert_eq!(
        Store::open(database)
            .expect("reopen")
            .obligation_state(ask_message_id)
            .expect("state"),
        ObligationState::Cancelled
    );
    recovered_daemon.kill().expect("stop recovered kelpied");
    recovered_daemon.wait().expect("reap recovered kelpied");
}

#[test]
fn kill_after_cancellation_write_recovers_unknown_without_resend() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let parsed_socket = directory.path().join("parsed.sock");
    let (waiting, _owing, ask_message_id) = seed_open_ask(&database);
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let parsed_listener = UnixListener::bind(&parsed_socket).expect("bind parsed harness");
    let first_herdr = spawn_withholding_cancellation_herdr(&herdr_socket, &parsed_socket);
    let mut first_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!(
            "{DAEMON_BOUND},{}",
            "cancellation_after_write_before_response"
        ),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    let client = send_cancel(
        &kelpie_socket,
        &waiting.logical_agent_id.to_string(),
        ask_message_id,
    );
    accept_parsed(&parsed_listener, "cancellation parsed");
    let written = accept_point(&fault_listener, "cancellation_after_write_before_response");
    first_daemon.kill().expect("kill first kelpied");
    first_daemon.wait().expect("reap first kelpied");
    drop(written);
    client.join().expect("cancel client");
    first_herdr.join().expect("first Herdr fixture");
    // The bytes crossed: the attempt is submitted, the outcome is not committed.
    let (operation_outcome, delivery_outcome): (String, String) = Connection::open(&database)
        .expect("open state database")
        .query_row(
            "SELECT o.outcome, d.outcome
             FROM operations o
             JOIN deliveries d ON d.operation_id = o.id
             WHERE o.kind = 'prompt' AND o.intent_json LIKE '%\"audience\":\"waiting\"%'
             ORDER BY o.created_at_ms DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("durable cancellation state");
    assert_eq!(operation_outcome, "pending");
    assert_eq!(delivery_outcome, "submitted");
    recover_marking_unknown(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &fault_listener,
        ask_message_id,
    );
}

#[test]
fn kill_after_cancellation_response_recovers_unknown_without_resend() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (waiting, _owing, ask_message_id) = seed_open_ask(&database);
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let first_herdr = spawn_responding_cancellation_herdr(&herdr_socket);
    let mut first_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!(
            "{DAEMON_BOUND},{}",
            "cancellation_after_response_before_commit"
        ),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    let client = send_cancel(
        &kelpie_socket,
        &waiting.logical_agent_id.to_string(),
        ask_message_id,
    );
    let responded = accept_point(&fault_listener, "cancellation_after_response_before_commit");
    first_daemon.kill().expect("kill first kelpied");
    first_daemon.wait().expect("reap first kelpied");
    drop(responded);
    client.join().expect("cancel client");
    first_herdr.join().expect("first Herdr fixture");
    // Acceptance was never committed locally: the delivery stays submitted.
    let (operation_outcome, delivery_outcome): (String, String) = Connection::open(&database)
        .expect("open state database")
        .query_row(
            "SELECT o.outcome, d.outcome
             FROM operations o
             JOIN deliveries d ON d.operation_id = o.id
             WHERE o.kind = 'prompt' AND o.intent_json LIKE '%\"audience\":\"waiting\"%'
             ORDER BY o.created_at_ms DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("durable cancellation state");
    assert_eq!(operation_outcome, "pending");
    assert_eq!(delivery_outcome, "submitted");
    recover_marking_unknown(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &fault_listener,
        ask_message_id,
    );
}

const OWING_SUBMITTED: &str = "owing_cancellation_after_submitted_before_write";

fn spawn_asker_then_unwritten_owing_herdr(socket: &Path) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind asker-then-owing fake Herdr");
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
                "agent.prompt",
                serde_json::json!({
                    "type":"agent_prompted",
                    "agent":observation("waiting", "w1:p1", "term-waiting")
                }),
            ),
        ] {
            let (mut stream, _) = listener.accept().expect("accept Herdr exchange");
            respond(&mut stream, method, &result);
        }
        let (mut unwritten_prompt, _) = listener.accept().expect("accept owing prompt connection");
        let mut bytes = Vec::new();
        unwritten_prompt
            .read_to_end(&mut bytes)
            .expect("read owing prompt connection to process death");
        assert!(
            bytes.is_empty(),
            "owing agent.prompt bytes crossed the fault point"
        );
    })
}

fn spawn_withholding_owing_cancellation_herdr(
    socket: &Path,
    parsed_socket: &Path,
) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind withholding owing fake Herdr");
    let parsed_socket = parsed_socket.to_path_buf();
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
                "agent.prompt",
                serde_json::json!({
                    "type":"agent_prompted",
                    "agent":observation("waiting", "w1:p1", "term-waiting")
                }),
            ),
        ] {
            let (mut stream, _) = listener.accept().expect("accept Herdr exchange");
            respond(&mut stream, method, &result);
        }
        let (prompt_stream, _) = listener.accept().expect("accept owing cancellation prompt");
        let mut line = String::new();
        BufReader::new(prompt_stream.try_clone().expect("clone prompt stream"))
            .read_line(&mut line)
            .expect("read complete owing cancellation prompt");
        let request: Value =
            serde_json::from_str(&line).expect("complete owing cancellation prompt JSON");
        assert_eq!(request["method"], "agent.prompt");
        assert_eq!(request["params"]["target"], "w1:p2");
        let envelope = request["params"]["text"]
            .as_str()
            .expect("owing cancellation text");
        assert!(envelope.contains("<kelpie-system cancellation owing="));
        assert!(!envelope.contains("reply-to"));
        UnixStream::connect(parsed_socket)
            .expect("connect parsed signal")
            .write_all(b"owing cancellation parsed\n")
            .expect("report parsed owing cancellation");
        let mut remainder = Vec::new();
        prompt_stream
            .try_clone()
            .expect("clone withheld prompt")
            .read_to_end(&mut remainder)
            .expect("wait for daemon death without response");
        assert!(remainder.is_empty());
    })
}

fn spawn_responding_owing_cancellation_herdr(socket: &Path) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind responding owing fake Herdr");
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
                "agent.prompt",
                serde_json::json!({
                    "type":"agent_prompted",
                    "agent":observation("waiting", "w1:p1", "term-waiting")
                }),
            ),
            (
                "agent.prompt",
                serde_json::json!({
                    "type":"agent_prompted",
                    "agent":observation("owing", "w1:p2", "term-owing")
                }),
            ),
        ] {
            let (mut stream, _) = listener.accept().expect("accept Herdr exchange");
            respond(&mut stream, method, &result);
        }
    })
}

fn recover_marking_owing_unknown(
    database: &Path,
    kelpie_socket: &Path,
    herdr_socket: &Path,
    fault_socket: &Path,
    fault_listener: &UnixListener,
    ask_message_id: MessageId,
) {
    fs::remove_file(kelpie_socket).expect("remove killed daemon socket");
    fs::remove_file(herdr_socket).expect("remove first Herdr socket");
    let recovery_herdr = spawn_recovery_herdr(herdr_socket);
    let mut recovered_daemon = spawn_kelpied(
        database,
        kelpie_socket,
        herdr_socket,
        fault_socket,
        DAEMON_BOUND,
    );
    let mut recovered_bound = accept_point(fault_listener, DAEMON_BOUND);
    recovery_herdr.join().expect("recovery Herdr fixture");
    recovered_bound
        .write_all(b"x")
        .expect("release recovered daemon");
    let (operation_outcome, delivery_outcome): (String, String) = Connection::open(database)
        .expect("open state database")
        .query_row(
            "SELECT o.outcome, d.outcome
             FROM operations o
             JOIN deliveries d ON d.operation_id = o.id
             WHERE o.kind = 'prompt' AND o.intent_json LIKE '%\"audience\":\"owing\"%'
             ORDER BY o.created_at_ms DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("recovered owing cancellation delivery");
    assert_eq!(operation_outcome, "unknown");
    assert_eq!(delivery_outcome, "unknown");
    assert_eq!(
        Store::open(database)
            .expect("reopen")
            .obligation_state(ask_message_id)
            .expect("state"),
        ObligationState::Cancelled
    );
    recovered_daemon.kill().expect("stop recovered kelpied");
    recovered_daemon.wait().expect("reap recovered kelpied");
}

#[test]
fn kill_after_owing_cancellation_submitted_settles_without_resend() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (waiting, _owing, ask_message_id) = seed_open_ask(&database);
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let first_herdr = spawn_asker_then_unwritten_owing_herdr(&herdr_socket);
    let mut first_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},{OWING_SUBMITTED}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    let client = send_cancel(
        &kelpie_socket,
        &waiting.logical_agent_id.to_string(),
        ask_message_id,
    );
    let submitted = accept_point(&fault_listener, OWING_SUBMITTED);
    first_daemon.kill().expect("kill first kelpied");
    first_daemon.wait().expect("reap first kelpied");
    drop(submitted);
    client.join().expect("cancel client");
    first_herdr.join().expect("first Herdr fixture");

    let (obligation_state, delivery_outcome, operation_outcome): (String, String, String) =
        Connection::open(&database)
            .expect("open state database")
            .query_row(
                "SELECT ob.state, d.outcome, o.outcome
                 FROM obligations ob
                 JOIN operations o ON o.intent_json LIKE '%\"audience\":\"owing\"%'
                 JOIN deliveries d ON d.operation_id = o.id
                 WHERE ob.ask_message_id = ?1",
                [ask_message_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("durable owing cancellation state");
    assert_eq!(obligation_state, "cancelled");
    assert_eq!(delivery_outcome, "submitted");
    assert_eq!(operation_outcome, "pending");
    recover_marking_owing_unknown(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &fault_listener,
        ask_message_id,
    );
}

#[test]
fn kill_after_owing_cancellation_write_recovers_unknown_without_resend() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let parsed_socket = directory.path().join("parsed.sock");
    let (waiting, _owing, ask_message_id) = seed_open_ask(&database);
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let parsed_listener = UnixListener::bind(&parsed_socket).expect("bind parsed harness");
    let first_herdr = spawn_withholding_owing_cancellation_herdr(&herdr_socket, &parsed_socket);
    let mut first_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},owing_cancellation_after_write_before_response"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    let client = send_cancel(
        &kelpie_socket,
        &waiting.logical_agent_id.to_string(),
        ask_message_id,
    );
    accept_parsed(&parsed_listener, "owing cancellation parsed");
    let written = accept_point(
        &fault_listener,
        "owing_cancellation_after_write_before_response",
    );
    first_daemon.kill().expect("kill first kelpied");
    first_daemon.wait().expect("reap first kelpied");
    drop(written);
    client.join().expect("cancel client");
    first_herdr.join().expect("first Herdr fixture");
    let (operation_outcome, delivery_outcome): (String, String) = Connection::open(&database)
        .expect("open state database")
        .query_row(
            "SELECT o.outcome, d.outcome
             FROM operations o
             JOIN deliveries d ON d.operation_id = o.id
             WHERE o.kind = 'prompt' AND o.intent_json LIKE '%\"audience\":\"owing\"%'
             ORDER BY o.created_at_ms DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("durable owing cancellation state");
    assert_eq!(operation_outcome, "pending");
    assert_eq!(delivery_outcome, "submitted");
    recover_marking_owing_unknown(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &fault_listener,
        ask_message_id,
    );
}

#[test]
fn kill_after_owing_cancellation_response_recovers_unknown_without_resend() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (waiting, _owing, ask_message_id) = seed_open_ask(&database);
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let first_herdr = spawn_responding_owing_cancellation_herdr(&herdr_socket);
    let mut first_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},owing_cancellation_after_response_before_commit"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    let client = send_cancel(
        &kelpie_socket,
        &waiting.logical_agent_id.to_string(),
        ask_message_id,
    );
    let responded = accept_point(
        &fault_listener,
        "owing_cancellation_after_response_before_commit",
    );
    first_daemon.kill().expect("kill first kelpied");
    first_daemon.wait().expect("reap first kelpied");
    drop(responded);
    client.join().expect("cancel client");
    first_herdr.join().expect("first Herdr fixture");
    let (operation_outcome, delivery_outcome): (String, String) = Connection::open(&database)
        .expect("open state database")
        .query_row(
            "SELECT o.outcome, d.outcome
             FROM operations o
             JOIN deliveries d ON d.operation_id = o.id
             WHERE o.kind = 'prompt' AND o.intent_json LIKE '%\"audience\":\"owing\"%'
             ORDER BY o.created_at_ms DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("durable owing cancellation state");
    assert_eq!(operation_outcome, "pending");
    assert_eq!(delivery_outcome, "submitted");
    recover_marking_owing_unknown(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &fault_listener,
        ask_message_id,
    );
}
