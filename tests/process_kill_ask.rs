//! Real-`kelpied` process-kill coverage for an ask before prompt write.

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
const ASK_SUBMITTED: &str = "ask_after_submitted_before_write";
const ASK_WRITTEN: &str = "ask_after_write_before_response";
const ASK_RESPONDED: &str = "ask_after_response_before_commit";

fn intent(name: &str, pane: &str, terminal: &str, key: &str) -> StartIntent {
    StartIntent {
        public_name: name.into(),
        logical_agent_id: None,
        parent: Parent::Parentless,
        herdr_session: "ask-fault-test".into(),
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

fn seed_ready_agents(database: &Path) -> (DeclaredStart, DeclaredStart) {
    let mut store = Store::open(database).expect("open seed store");
    let waiting = store
        .declare_start(&intent("waiting", "w1:p1", "term-waiting", "waiting-start"))
        .expect("declare waiting agent");
    let owing = store
        .declare_start(&intent("owing", "w1:p2", "term-owing", "owing-start"))
        .expect("declare owing agent");
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
    (waiting, owing)
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

fn authoritative_agents() -> Value {
    serde_json::json!([
        observation("waiting", "w1:p1", "term-waiting"),
        observation("owing", "w1:p2", "term-owing")
    ])
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
                    "agent":observation("owing", "w1:p2", "term-owing")
                }),
            ),
        ] {
            let (mut stream, _) = listener.accept().expect("accept Herdr exchange");
            respond(&mut stream, method, &result);
        }
    })
}

fn spawn_withholding_ask_herdr(socket: &Path, parsed_socket: &Path) -> thread::JoinHandle<()> {
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
        let (prompt_stream, _) = listener.accept().expect("accept ask prompt");
        let mut line = String::new();
        BufReader::new(prompt_stream.try_clone().expect("clone prompt stream"))
            .read_line(&mut line)
            .expect("read complete ask prompt");
        let request: Value = serde_json::from_str(&line).expect("complete ask prompt JSON");
        assert_eq!(request["method"], "agent.prompt");
        assert_eq!(request["params"]["target"], "w1:p2");
        let envelope = request["params"]["text"].as_str().expect("ask prompt text");
        assert!(
            envelope.contains("reply-to="),
            "ask envelope must carry durable reply-to handle: {envelope}"
        );
        assert!(
            envelope.contains("survive prompt uncertainty"),
            "ask envelope body missing: {envelope}"
        );
        assert!(
            envelope.starts_with("<kelpie from="),
            "ask envelope shape: {envelope}"
        );
        UnixStream::connect(parsed_socket)
            .expect("connect parsed signal")
            .write_all(b"ask parsed\n")
            .expect("report parsed ask");
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

fn send_ask(
    socket: &Path,
    waiting: DeclaredStart,
    owing: DeclaredStart,
) -> thread::JoinHandle<Vec<u8>> {
    let socket = socket.to_path_buf();
    thread::spawn(move || {
        let mut stream = UnixStream::connect(socket).expect("connect Kelpie client");
        serde_json::to_writer(
            &mut stream,
            &serde_json::json!({
                "id":"kill-ask",
                "method":"ask",
                "params":{
                    "sender":waiting.logical_agent_id,
                    "recipient":owing.logical_agent_id,
                    "recipient_incarnation":owing.incarnation_id,
                    "body":"survive prompt uncertainty",
                    "idempotency_key":"fault-ask"
                }
            }),
        )
        .expect("write ask request");
        stream.write_all(b"\n").expect("finish ask request");
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

fn durable_ask_state(database: &Path) -> (String, String, String, String, String, i64, i64) {
    Connection::open(database)
        .expect("open state database")
        .query_row(
            "SELECT m.id, o.outcome, a.phase, d.outcome, ob.state,
                    a.attempt_number, d.attempt_number
             FROM operations o
             JOIN operation_attempts a ON a.operation_id = o.id
             JOIN deliveries d ON d.operation_id = o.id
             JOIN messages m ON m.id = d.message_id
             JOIN obligations ob ON ob.ask_message_id = m.id
             WHERE o.kind = 'prompt' AND m.kind = 'ask'",
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
        .expect("durable ask state")
}

#[test]
fn kill_after_ask_submitted_recovers_unknown_without_resend_and_keeps_obligation_open() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (waiting, owing) = seed_ready_agents(&database);
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let first_herdr = spawn_first_herdr(&herdr_socket);
    let mut first_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},{ASK_SUBMITTED}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    let client = send_ask(&kelpie_socket, waiting, owing);
    let submitted = accept_point(&fault_listener, ASK_SUBMITTED);
    first_daemon.kill().expect("kill first kelpied");
    first_daemon.wait().expect("reap first kelpied");
    drop(submitted);
    assert!(client.join().expect("ask client").is_empty());
    first_herdr.join().expect("first Herdr fixture");
    let before_recovery = durable_ask_state(&database);
    assert_eq!(
        (
            before_recovery.1.as_str(),
            before_recovery.2.as_str(),
            before_recovery.3.as_str(),
            before_recovery.4.as_str(),
            before_recovery.5,
            before_recovery.6,
        ),
        ("pending", "submitted", "submitted", "open", 1, 1,)
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
    let mut recovered_bound = accept_point(&fault_listener, DAEMON_BOUND);
    recovery_herdr.join().expect("recovery Herdr fixture");
    recovered_bound
        .write_all(b"x")
        .expect("release recovered daemon");
    let pending = send_request(
        &kelpie_socket,
        &serde_json::json!({
            "id":"pending-after-kill",
            "method":"pending",
            "params":{"agent_id":owing.logical_agent_id}
        }),
    );
    recovered_daemon.kill().expect("kill recovered kelpied");
    recovered_daemon.wait().expect("reap recovered kelpied");

    let after_recovery = durable_ask_state(&database);
    assert_eq!(after_recovery.0, before_recovery.0);
    assert_eq!(
        (
            after_recovery.1.as_str(),
            after_recovery.2.as_str(),
            after_recovery.3.as_str(),
            after_recovery.4.as_str(),
            after_recovery.5,
            after_recovery.6,
        ),
        ("unknown", "unknown", "unknown", "open", 1, 1,)
    );
    assert_eq!(
        pending["result"],
        serde_json::json!([{
            "ask_message_id":after_recovery.0,
            "waiting_agent_id":waiting.logical_agent_id,
            "state":"open"
        }])
    );
}

#[test]
fn kill_after_ask_write_recovers_unknown_without_resend_and_keeps_obligation_open() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let parsed_socket = directory.path().join("parsed.sock");
    let (waiting, owing) = seed_ready_agents(&database);
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let parsed_listener = UnixListener::bind(&parsed_socket).expect("bind parsed harness");
    let first_herdr = spawn_withholding_ask_herdr(&herdr_socket, &parsed_socket);
    let mut first_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},{ASK_WRITTEN}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    let client = send_ask(&kelpie_socket, waiting, owing);
    let written = accept_point(&fault_listener, ASK_WRITTEN);
    accept_parsed(&parsed_listener, "ask parsed");
    first_daemon.kill().expect("kill first kelpied");
    first_daemon.wait().expect("reap first kelpied");
    drop(written);
    assert!(client.join().expect("ask client").is_empty());
    first_herdr.join().expect("withholding Herdr fixture");
    let before_recovery = durable_ask_state(&database);
    assert_eq!(
        (
            before_recovery.1.as_str(),
            before_recovery.2.as_str(),
            before_recovery.3.as_str(),
            before_recovery.4.as_str(),
            before_recovery.5,
            before_recovery.6,
        ),
        ("pending", "submitted", "submitted", "open", 1, 1)
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
    let mut recovered_bound = accept_point(&fault_listener, DAEMON_BOUND);
    recovery_herdr.join().expect("recovery Herdr fixture");
    recovered_bound
        .write_all(b"x")
        .expect("release recovered daemon");
    let pending = send_request(
        &kelpie_socket,
        &serde_json::json!({
            "id":"pending-after-written-kill",
            "method":"pending",
            "params":{"agent_id":owing.logical_agent_id}
        }),
    );
    recovered_daemon.kill().expect("kill recovered kelpied");
    recovered_daemon.wait().expect("reap recovered kelpied");

    let after_recovery = durable_ask_state(&database);
    assert_eq!(after_recovery.0, before_recovery.0);
    assert_eq!(
        (
            after_recovery.1.as_str(),
            after_recovery.2.as_str(),
            after_recovery.3.as_str(),
            after_recovery.4.as_str(),
            after_recovery.5,
            after_recovery.6,
        ),
        ("unknown", "unknown", "unknown", "open", 1, 1)
    );
    assert_eq!(
        pending["result"],
        serde_json::json!([{
            "ask_message_id":after_recovery.0,
            "waiting_agent_id":waiting.logical_agent_id,
            "state":"open"
        }])
    );
}

#[test]
fn kill_after_ask_acceptance_recovers_unknown_without_resend_and_keeps_obligation_open() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (waiting, owing) = seed_ready_agents(&database);
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let first_herdr = spawn_responding_herdr(&herdr_socket);
    let mut first_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},{ASK_RESPONDED}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    let client = send_ask(&kelpie_socket, waiting, owing);
    let responded = accept_point(&fault_listener, ASK_RESPONDED);
    first_daemon.kill().expect("kill first kelpied");
    first_daemon.wait().expect("reap first kelpied");
    drop(responded);
    assert!(client.join().expect("ask client").is_empty());
    first_herdr.join().expect("responding Herdr fixture");
    let before_recovery = durable_ask_state(&database);
    assert_eq!(
        (
            before_recovery.1.as_str(),
            before_recovery.2.as_str(),
            before_recovery.3.as_str(),
            before_recovery.4.as_str(),
            before_recovery.5,
            before_recovery.6,
        ),
        ("pending", "submitted", "submitted", "open", 1, 1,)
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
    let mut recovered_bound = accept_point(&fault_listener, DAEMON_BOUND);
    recovery_herdr.join().expect("recovery Herdr fixture");
    recovered_bound
        .write_all(b"x")
        .expect("release recovered daemon");
    let pending = send_request(
        &kelpie_socket,
        &serde_json::json!({
            "id":"pending-after-accepted-kill",
            "method":"pending",
            "params":{"agent_id":owing.logical_agent_id}
        }),
    );
    recovered_daemon.kill().expect("kill recovered kelpied");
    recovered_daemon.wait().expect("reap recovered kelpied");

    let after_recovery = durable_ask_state(&database);
    assert_eq!(after_recovery.0, before_recovery.0);
    assert_eq!(
        (
            after_recovery.1.as_str(),
            after_recovery.2.as_str(),
            after_recovery.3.as_str(),
            after_recovery.4.as_str(),
            after_recovery.5,
            after_recovery.6,
        ),
        ("unknown", "unknown", "unknown", "open", 1, 1,)
    );
    assert_eq!(
        pending["result"],
        serde_json::json!([{
            "ask_message_id":after_recovery.0,
            "waiting_agent_id":waiting.logical_agent_id,
            "state":"open"
        }])
    );
}
