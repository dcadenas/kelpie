//! Real-`kelpied` process-kill coverage for final-reply delivery boundaries.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;

use kelpie::domain::{
    InitialMessageIntent, InitialMessageKind, LogicalAgentId, MessageId, ObligationState, Parent,
    StartIntent,
};
use kelpie::herdr::AgentObservation;
use kelpie::store::{DeclaredStart, Store};
use rusqlite::Connection;
use serde_json::Value;

const DAEMON_BOUND: &str = "daemon_bound";
const REPLY_SUBMITTED: &str = "reply_after_submitted_before_write";
const REPLY_WRITTEN: &str = "reply_after_write_before_response";
const REPLY_RESPONDED: &str = "reply_after_response_before_commit";

fn intent(name: &str, pane: &str, terminal: &str, key: &str) -> StartIntent {
    StartIntent {
        public_name: name.into(),
        logical_agent_id: None,
        parent: Parent::Parentless,
        herdr_session: "reply-fault-test".into(),
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
            "seed-ask",
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

fn spawn_responding_herdr(socket: &Path) -> thread::JoinHandle<()> {
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

fn spawn_withholding_reply_herdr(socket: &Path, parsed_socket: &Path) -> thread::JoinHandle<()> {
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
        let (prompt_stream, _) = listener.accept().expect("accept reply prompt");
        let mut line = String::new();
        BufReader::new(prompt_stream.try_clone().expect("clone prompt stream"))
            .read_line(&mut line)
            .expect("read complete reply prompt");
        let request: Value = serde_json::from_str(&line).expect("complete reply prompt JSON");
        assert_eq!(request["method"], "agent.prompt");
        assert_eq!(request["params"]["target"], "w1:p1");
        let envelope = request["params"]["text"].as_str().expect("reply text");
        assert!(envelope.contains(" final>"));
        UnixStream::connect(parsed_socket)
            .expect("connect parsed signal")
            .write_all(b"reply parsed\n")
            .expect("report parsed reply");
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

fn send_final_reply(
    socket: &Path,
    owing: LogicalAgentId,
    ask_message_id: MessageId,
) -> thread::JoinHandle<Vec<u8>> {
    let socket = socket.to_path_buf();
    thread::spawn(move || {
        let mut stream = UnixStream::connect(socket).expect("connect Kelpie client");
        serde_json::to_writer(
            &mut stream,
            &serde_json::json!({
                "id":"kill-reply",
                "method":"reply",
                "params":{
                    "reply_to":ask_message_id,
                    "requester_agent_id":owing,
                    "body":"final answer",
                    "disposition":"final",
                    "idempotency_key":"fault-final"
                }
            }),
        )
        .expect("write reply request");
        stream.write_all(b"\n").expect("finish reply request");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .expect("read until daemon death");
        response
    })
}

fn send_request(socket: &Path, request: &Value) -> Value {
    let mut stream = UnixStream::connect(socket).expect("connect Kelpie client");
    serde_json::to_writer(&mut stream, request).expect("write request");
    stream.write_all(b"\n").expect("finish request");
    let mut line = String::new();
    BufReader::new(stream)
        .read_line(&mut line)
        .expect("read response");
    serde_json::from_str(&line).expect("response JSON")
}

fn durable_reply_state(database: &Path) -> (String, String, String, String, String) {
    Connection::open(database)
        .expect("open state database")
        .query_row(
            "SELECT m.id, o.outcome, a.phase, d.outcome, ob.state
             FROM operations o
             JOIN operation_attempts a ON a.operation_id = o.id
             JOIN deliveries d ON d.operation_id = o.id
             JOIN messages m ON m.id = d.message_id
             JOIN obligations ob ON ob.ask_message_id = m.reply_to_message_id
             WHERE o.kind = 'prompt' AND m.kind = 'reply' AND m.disposition = 'final'",
            [],
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
        .expect("durable reply state")
}

fn recover_and_assert_unknown_open(
    database: &Path,
    kelpie_socket: &Path,
    herdr_socket: &Path,
    fault_socket: &Path,
    fault_listener: &UnixListener,
    owing: DeclaredStart,
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
    let pending = send_request(
        kelpie_socket,
        &serde_json::json!({
            "id":"pending-after-recovery",
            "method":"pending",
            "params":{"agent_id":owing.logical_agent_id}
        }),
    );
    assert_eq!(
        pending["result"][0]["ask_message_id"],
        ask_message_id.to_string()
    );
    assert_eq!(pending["result"][0]["state"], "open");
    let after = durable_reply_state(database);
    assert_eq!(
        (
            after.1.as_str(),
            after.2.as_str(),
            after.3.as_str(),
            after.4.as_str()
        ),
        ("unknown", "unknown", "unknown", "open")
    );
    recovered_daemon.kill().expect("stop recovered kelpied");
    recovered_daemon.wait().expect("reap recovered kelpied");
}

#[test]
fn kill_after_reply_submitted_recovers_unknown_without_resend_and_keeps_obligation_open() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (_waiting, owing, ask_message_id) = seed_open_ask(&database);
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let first_herdr = spawn_first_herdr(&herdr_socket);
    let mut first_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},{REPLY_SUBMITTED}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    let client = send_final_reply(&kelpie_socket, owing.logical_agent_id, ask_message_id);
    let submitted = accept_point(&fault_listener, REPLY_SUBMITTED);
    first_daemon.kill().expect("kill first kelpied");
    first_daemon.wait().expect("reap first kelpied");
    drop(submitted);
    assert!(client.join().expect("reply client").is_empty());
    first_herdr.join().expect("first Herdr fixture");
    let before = durable_reply_state(&database);
    assert_eq!(
        (
            before.1.as_str(),
            before.2.as_str(),
            before.3.as_str(),
            before.4.as_str()
        ),
        ("pending", "submitted", "submitted", "open")
    );
    recover_and_assert_unknown_open(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &fault_listener,
        owing,
        ask_message_id,
    );
}

#[test]
fn kill_after_reply_write_recovers_unknown_without_resend_and_keeps_obligation_open() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let parsed_socket = directory.path().join("parsed.sock");
    let (_waiting, owing, ask_message_id) = seed_open_ask(&database);
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let parsed_listener = UnixListener::bind(&parsed_socket).expect("bind parsed signal");
    let first_herdr = spawn_withholding_reply_herdr(&herdr_socket, &parsed_socket);
    let mut first_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},{REPLY_WRITTEN}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    let client = send_final_reply(&kelpie_socket, owing.logical_agent_id, ask_message_id);
    accept_parsed(&parsed_listener, "reply parsed");
    let written = accept_point(&fault_listener, REPLY_WRITTEN);
    first_daemon.kill().expect("kill first kelpied");
    first_daemon.wait().expect("reap first kelpied");
    drop(written);
    assert!(client.join().expect("reply client").is_empty());
    first_herdr.join().expect("first Herdr fixture");
    recover_and_assert_unknown_open(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &fault_listener,
        owing,
        ask_message_id,
    );
}

#[test]
fn kill_after_reply_acceptance_recovers_unknown_without_resend_and_keeps_obligation_open() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (_waiting, owing, ask_message_id) = seed_open_ask(&database);
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let first_herdr = spawn_responding_herdr(&herdr_socket);
    let mut first_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},{REPLY_RESPONDED}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    let client = send_final_reply(&kelpie_socket, owing.logical_agent_id, ask_message_id);
    let responded = accept_point(&fault_listener, REPLY_RESPONDED);
    first_daemon.kill().expect("kill first kelpied");
    first_daemon.wait().expect("reap first kelpied");
    drop(responded);
    assert!(client.join().expect("reply client").is_empty());
    first_herdr.join().expect("first Herdr fixture");
    let before = durable_reply_state(&database);
    assert_eq!(
        (
            before.1.as_str(),
            before.2.as_str(),
            before.3.as_str(),
            before.4.as_str()
        ),
        ("pending", "submitted", "submitted", "open")
    );
    // Obligation must remain open: acceptance was never committed locally.
    assert_eq!(
        Store::open(&database)
            .expect("reopen")
            .obligation_state(ask_message_id)
            .expect("state"),
        ObligationState::Open
    );
    recover_and_assert_unknown_open(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &fault_listener,
        owing,
        ask_message_id,
    );
}
