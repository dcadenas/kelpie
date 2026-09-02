//! Real-`kelpied` process-kill coverage for socket-inbox write boundaries.

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
use kelpie::store::{CreatedWaiter, DeclaredStart, Store};
use rusqlite::Connection;
use serde_json::Value;

const DAEMON_BOUND: &str = "daemon_bound";
const INBOX_BEFORE_WRITE: &str = "inbox_after_queued_before_write";
const INBOX_AFTER_WRITE: &str = "inbox_after_write_before_ack";
const INBOX_AFTER_ACK: &str = "inbox_after_ack_before_resolve";

fn intent(name: &str, pane: &str, terminal: &str, key: &str) -> StartIntent {
    StartIntent {
        public_name: name.into(),
        logical_agent_id: None,
        parent: Parent::Parentless,
        herdr_session: "inbox-fault-test".into(),
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
    serde_json::json!([observation("owing", "w1:p2", "term-owing")])
}

fn seed_open_ask(database: &Path) -> (CreatedWaiter, DeclaredStart, MessageId) {
    let mut store = Store::open(database).expect("open seed store");
    let waiter = store
        .register_socket_waiter("inbox", Parent::Parentless, "seed-waiter")
        .expect("register waiter");
    let owing = store
        .declare_start(&intent("owing", "w1:p2", "term-owing", "owing-start"))
        .expect("declare owing");
    store
        .begin_attempt(owing.operation_id, owing.incarnation_id, "seed-start")
        .expect("begin seed attempt");
    store
        .accept_start_ready(
            owing.operation_id,
            owing.incarnation_id,
            &observation("owing", "w1:p2", "term-owing"),
            None,
        )
        .expect("accept seed readiness");
    let ask = store
        .create_ask(
            waiter.logical_agent_id,
            owing.logical_agent_id,
            owing.incarnation_id,
            "need a final reply",
            "seed-ask",
        )
        .expect("seed ask");
    (waiter, owing, ask.message_id)
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

fn spawn_startup_herdr(socket: &Path) -> thread::JoinHandle<()> {
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
        ] {
            let (mut stream, _) = listener.accept().expect("accept startup exchange");
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

fn send_final_reply(socket: &Path, owing: LogicalAgentId, ask_message_id: MessageId) -> Value {
    send_request(
        socket,
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
}

fn read_json(stream: &mut UnixStream) -> Value {
    let mut line = String::new();
    BufReader::new(stream.try_clone().expect("clone"))
        .read_line(&mut line)
        .expect("read json line");
    serde_json::from_str(&line).expect("json")
}

fn claim_inbox(socket: &Path, waiter: LogicalAgentId, id: &str) -> UnixStream {
    let mut stream = UnixStream::connect(socket).expect("connect inbox");
    serde_json::to_writer(
        &mut stream,
        &serde_json::json!({
            "id": id,
            "method": "inbox.claim",
            "params": {"logical_agent_id": waiter},
        }),
    )
    .expect("write claim");
    stream.write_all(b"\n").expect("finish claim");
    let claimed = read_json(&mut stream);
    assert_eq!(claimed["result"]["claimed"], true);
    stream
}

fn ack_delivery(stream: &mut UnixStream, message_id: &Value, id: &str) {
    serde_json::to_writer(
        &mut *stream,
        &serde_json::json!({
            "id": id,
            "method": "inbox.ack",
            "params": {"message_id": message_id},
        }),
    )
    .expect("write ack");
    stream.write_all(b"\n").expect("finish ack");
}

fn durable_inbox_state(database: &Path) -> (String, String, i64) {
    Connection::open(database)
        .expect("open state database")
        .query_row(
            "SELECT d.outcome, ob.state,
                    (SELECT COUNT(*) FROM deliveries
                      WHERE delivery_transport = 'socket_inbox')
             FROM deliveries d
             JOIN messages m ON m.id = d.message_id
             JOIN obligations ob ON ob.ask_message_id = m.reply_to_message_id
             WHERE d.delivery_transport = 'socket_inbox'
               AND m.kind = 'reply'
               AND m.disposition = 'final'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("durable inbox state")
}

fn reply_message_id(database: &Path) -> String {
    Connection::open(database)
        .expect("open state database")
        .query_row(
            "SELECT m.id FROM messages m
             JOIN deliveries d ON d.message_id = m.id
             WHERE m.kind = 'reply' AND d.delivery_transport = 'socket_inbox'",
            [],
            |row| row.get(0),
        )
        .expect("reply id")
}

fn kill_daemon(mut daemon: Child) {
    daemon.kill().expect("kill kelpied");
    daemon.wait().expect("reap kelpied");
}

fn recover_daemon(
    database: &Path,
    kelpie_socket: &Path,
    herdr_socket: &Path,
    fault_socket: &Path,
    fault_listener: &UnixListener,
) -> Child {
    fs::remove_file(kelpie_socket).expect("remove killed daemon socket");
    fs::remove_file(herdr_socket).expect("remove first Herdr socket");
    let recovery_herdr = spawn_startup_herdr(herdr_socket);
    let recovered = spawn_kelpied(
        database,
        kelpie_socket,
        herdr_socket,
        fault_socket,
        DAEMON_BOUND,
    );
    let mut bound = accept_point(fault_listener, DAEMON_BOUND);
    recovery_herdr.join().expect("recovery Herdr fixture");
    bound.write_all(b"x").expect("release recovered daemon");
    recovered
}

fn ack_until_resolved(kelpie_socket: &Path, waiter: LogicalAgentId, expected_reply: &str) {
    let mut inbox = claim_inbox(kelpie_socket, waiter, "recover-claim");
    let delivery = read_json(&mut inbox);
    assert_eq!(delivery["method"], "inbox.delivery");
    assert_eq!(delivery["params"]["message_id"], expected_reply);
    ack_delivery(&mut inbox, &delivery["params"]["message_id"], "recover-ack");
    let ack = read_json(&mut inbox);
    assert_eq!(ack["result"]["outcome"], "accepted");
}

#[test]
fn kill_before_inbox_write_keeps_queued_without_bytes_or_resend() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (waiter, owing, ask_message_id) = seed_open_ask(&database);
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let first_herdr = spawn_startup_herdr(&herdr_socket);
    let first_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},{INBOX_BEFORE_WRITE}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    first_herdr.join().expect("first Herdr fixture");

    let inbox = claim_inbox(&kelpie_socket, waiter.logical_agent_id, "claim-1");
    let remainder = thread::spawn(move || {
        let mut bytes = Vec::new();
        BufReader::new(inbox)
            .read_to_end(&mut bytes)
            .expect("read until daemon death");
        bytes
    });
    let replied = send_final_reply(&kelpie_socket, owing.logical_agent_id, ask_message_id);
    assert_eq!(replied["result"]["delivery_outcome"], "queued");
    assert_eq!(replied["result"]["obligation_state"], "open");
    assert!(replied["result"]["operation_id"].is_null());
    let submitted = accept_point(&fault_listener, INBOX_BEFORE_WRITE);
    kill_daemon(first_daemon);
    drop(submitted);
    let leftover = remainder.join().expect("inbox client");
    assert!(
        leftover.is_empty(),
        "inbox.delivery bytes crossed the fault point: {}",
        String::from_utf8_lossy(&leftover)
    );
    let before = durable_inbox_state(&database);
    assert_eq!(
        (before.0.as_str(), before.1.as_str(), before.2),
        ("queued", "open", 1)
    );

    let mut recovered = recover_daemon(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &fault_listener,
    );
    let after = durable_inbox_state(&database);
    assert_eq!(
        (after.0.as_str(), after.1.as_str(), after.2),
        ("queued", "open", 1)
    );
    let reply_id = reply_message_id(&database);
    ack_until_resolved(&kelpie_socket, waiter.logical_agent_id, &reply_id);
    assert_eq!(
        Store::open(&database)
            .expect("reopen")
            .obligation_state(ask_message_id)
            .expect("state"),
        ObligationState::Resolved
    );
    assert_eq!(durable_inbox_state(&database).2, 1);
    recovered.kill().expect("stop recovered kelpied");
    recovered.wait().expect("reap recovered kelpied");
}

#[test]
fn kill_after_inbox_write_drains_the_same_queued_attempt() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (waiter, owing, ask_message_id) = seed_open_ask(&database);
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let first_herdr = spawn_startup_herdr(&herdr_socket);
    let first_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},{INBOX_AFTER_WRITE}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    first_herdr.join().expect("first Herdr fixture");

    let mut inbox = claim_inbox(&kelpie_socket, waiter.logical_agent_id, "claim-1");
    let replied = send_final_reply(&kelpie_socket, owing.logical_agent_id, ask_message_id);
    assert_eq!(replied["result"]["delivery_outcome"], "queued");
    let written = accept_point(&fault_listener, INBOX_AFTER_WRITE);
    let delivery = read_json(&mut inbox);
    assert_eq!(delivery["method"], "inbox.delivery");
    assert_eq!(delivery["params"]["kind"], "reply");
    assert_eq!(delivery["params"]["disposition"], "final");
    let reply_id = delivery["params"]["message_id"]
        .as_str()
        .expect("id")
        .to_string();
    kill_daemon(first_daemon);
    drop(written);
    let before = durable_inbox_state(&database);
    assert_eq!(
        (before.0.as_str(), before.1.as_str(), before.2),
        ("queued", "open", 1)
    );

    let mut recovered = recover_daemon(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &fault_listener,
    );
    assert_eq!(durable_inbox_state(&database).2, 1);
    ack_until_resolved(&kelpie_socket, waiter.logical_agent_id, &reply_id);
    assert_eq!(
        Store::open(&database)
            .expect("reopen")
            .obligation_state(ask_message_id)
            .expect("state"),
        ObligationState::Resolved
    );
    recovered.kill().expect("stop recovered kelpied");
    recovered.wait().expect("reap recovered kelpied");
}

#[test]
fn kill_after_inbox_ack_before_resolve_leaves_obligation_open() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (waiter, owing, ask_message_id) = seed_open_ask(&database);
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault harness");
    let first_herdr = spawn_startup_herdr(&herdr_socket);
    let first_daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},{INBOX_AFTER_ACK}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release daemon startup");
    first_herdr.join().expect("first Herdr fixture");

    let mut inbox = claim_inbox(&kelpie_socket, waiter.logical_agent_id, "claim-1");
    let replied = send_final_reply(&kelpie_socket, owing.logical_agent_id, ask_message_id);
    assert_eq!(replied["result"]["obligation_state"], "open");
    let delivery = read_json(&mut inbox);
    assert_eq!(delivery["method"], "inbox.delivery");
    let reply_id = delivery["params"]["message_id"].clone();
    ack_delivery(&mut inbox, &reply_id, "ack-1");
    let acked = accept_point(&fault_listener, INBOX_AFTER_ACK);
    kill_daemon(first_daemon);
    drop(acked);
    let before = durable_inbox_state(&database);
    assert_eq!(
        (before.0.as_str(), before.1.as_str(), before.2),
        ("queued", "open", 1)
    );
    assert_eq!(
        Store::open(&database)
            .expect("reopen")
            .obligation_state(ask_message_id)
            .expect("state"),
        ObligationState::Open
    );

    let mut recovered = recover_daemon(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &fault_listener,
    );
    ack_until_resolved(
        &kelpie_socket,
        waiter.logical_agent_id,
        reply_id.as_str().expect("id"),
    );
    assert_eq!(
        Store::open(&database)
            .expect("reopen")
            .obligation_state(ask_message_id)
            .expect("resolved"),
        ObligationState::Resolved
    );
    let second = send_request(
        &kelpie_socket,
        &serde_json::json!({
            "id":"second-final",
            "method":"reply",
            "params":{
                "reply_to":ask_message_id,
                "requester_agent_id":owing.logical_agent_id,
                "body":"again",
                "disposition":"final",
                "idempotency_key":"fault-final-2"
            }
        }),
    );
    assert_eq!(second["error"]["class"], "conflict");
    recovered.kill().expect("stop recovered kelpied");
    recovered.wait().expect("reap recovered kelpied");
}
