//! Real-`kelpied` socket-inbox conformance with a fake inbox client.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use kelpie::domain::{
    InitialMessageIntent, InitialMessageKind, LogicalAgentId, MessageId, ObligationState, Parent,
    StartIntent,
};
use kelpie::herdr::AgentObservation;
use kelpie::store::{CreatedWaiter, DeclaredStart, Store};
use rusqlite::Connection;
use serde_json::Value;

const DAEMON_BOUND: &str = "daemon_bound";

fn intent(name: &str, pane: &str, terminal: &str, key: &str) -> StartIntent {
    StartIntent {
        public_name: name.into(),
        logical_agent_id: None,
        parent: Parent::Parentless,
        herdr_session: "inbox-conformance".into(),
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

fn owing_agents() -> Value {
    let mut agent =
        serde_json::to_value(observation("owing", "w1:p2", "term-owing")).expect("agent");
    agent["agent_status"] = serde_json::json!("idle");
    serde_json::json!([agent])
}

fn seed_ready_owing(store: &mut Store) -> DeclaredStart {
    let owing = store
        .declare_start(&intent("owing", "w1:p2", "term-owing", "owing-start"))
        .expect("declare owing");
    store
        .begin_attempt(owing.operation_id, owing.incarnation_id, "seed-start")
        .expect("begin");
    store
        .accept_start_ready(
            owing.operation_id,
            owing.incarnation_id,
            &observation("owing", "w1:p2", "term-owing"),
            None,
        )
        .expect("ready");
    owing
}

fn seed_waiter_and_ask(database: &Path) -> (CreatedWaiter, DeclaredStart, MessageId) {
    let mut store = Store::open(database).expect("store");
    let waiter = store
        .register_socket_waiter("inbox", Parent::Parentless, "seed-waiter")
        .expect("waiter");
    let owing = seed_ready_owing(&mut store);
    let ask = store
        .create_ask(
            waiter.logical_agent_id,
            owing.logical_agent_id,
            owing.incarnation_id,
            "need a final reply",
            "seed-ask",
        )
        .expect("ask");
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
                    "snapshot":{"protocol":20,"panes":[],"agents":owing_agents()}
                }),
            ),
        ] {
            let (mut stream, _) = listener.accept().expect("accept startup");
            respond(&mut stream, method, &result);
        }
    })
}

fn spawn_prompt_herdr(socket: &Path, parsed: mpsc::Sender<Value>) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind prompt Herdr");
    thread::spawn(move || {
        loop {
            let (mut stream, _) = listener.accept().expect("accept Herdr");
            let mut line = String::new();
            BufReader::new(stream.try_clone().expect("clone"))
                .read_line(&mut line)
                .expect("read request");
            let request: Value = serde_json::from_str(&line).expect("request json");
            let result = match request["method"].as_str() {
                Some("ping") => {
                    serde_json::json!({"type":"pong","version":"test","protocol":20})
                }
                Some("session.snapshot") => serde_json::json!({
                    "type":"session_snapshot",
                    "snapshot":{"protocol":20,"panes":[],"agents":owing_agents()}
                }),
                Some("agent.prompt") => {
                    parsed.send(request.clone()).expect("send parsed");
                    serde_json::to_writer(
                        &mut stream,
                        &serde_json::json!({
                            "id": request["id"],
                            "result": {
                                "type": "agent_prompted",
                                "agent": observation("owing", "w1:p2", "term-owing")
                            }
                        }),
                    )
                    .expect("write prompt response");
                    stream.write_all(b"\n").expect("finish prompt");
                    return;
                }
                other => panic!("unexpected Herdr method {other:?}"),
            };
            serde_json::to_writer(
                &mut stream,
                &serde_json::json!({"id": request["id"], "result": result}),
            )
            .expect("write Herdr response");
            stream.write_all(b"\n").expect("finish response");
        }
    })
}

fn spawn_kelpied(
    database: &Path,
    kelpie_socket: &Path,
    herdr_socket: &Path,
    fault_socket: &Path,
) -> Child {
    Command::new(env!("CARGO_BIN_EXE_kelpied"))
        .arg("--database")
        .arg(database)
        .arg("--socket")
        .arg(kelpie_socket)
        .arg("--herdr-socket")
        .arg(herdr_socket)
        .env("KELPIE_TEST_FAULT_POINTS", DAEMON_BOUND)
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

fn read_json(reader: &mut BufReader<UnixStream>) -> Value {
    let mut line = String::new();
    reader.read_line(&mut line).expect("read json line");
    serde_json::from_str(&line).expect("json")
}

struct InboxConn {
    stream: UnixStream,
    reader: BufReader<UnixStream>,
}

fn claim_inbox(socket: &Path, waiter: LogicalAgentId, id: &str) -> InboxConn {
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
    let mut reader = BufReader::new(stream.try_clone().expect("clone inbox"));
    let claimed = read_json(&mut reader);
    assert_eq!(claimed["result"]["claimed"], true);
    InboxConn { stream, reader }
}

fn boot(
    database: &Path,
    kelpie_socket: &Path,
    herdr_socket: &Path,
    fault_socket: &Path,
) -> (Child, UnixListener) {
    let fault_listener = UnixListener::bind(fault_socket).expect("bind fault");
    let daemon = spawn_kelpied(database, kelpie_socket, herdr_socket, fault_socket);
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release");
    (daemon, fault_listener)
}

#[test]
fn occupant_final_resolves_only_on_socket_ack() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (waiter, owing, ask) = seed_waiter_and_ask(&database);
    let _herdr = spawn_startup_herdr(&herdr_socket);
    let (mut daemon, _fault) = boot(&database, &kelpie_socket, &herdr_socket, &fault_socket);

    let mut inbox = claim_inbox(&kelpie_socket, waiter.logical_agent_id, "claim");
    let replied = send_request(
        &kelpie_socket,
        &serde_json::json!({
            "id": "reply-1",
            "method": "reply",
            "params": {
                "reply_to": ask,
                "requester_agent_id": owing.logical_agent_id,
                "body": "done",
                "disposition": "final",
                "idempotency_key": "socket-final"
            }
        }),
    );
    assert_eq!(replied["result"]["delivery_outcome"], "queued");
    assert_eq!(replied["result"]["obligation_state"], "open");
    assert!(replied["result"]["recipient_incarnation"].is_null());
    let herdr_prompts: i64 = Connection::open(&database)
        .expect("db")
        .query_row(
            "SELECT COUNT(*) FROM deliveries
             WHERE delivery_transport = 'herdr_prompt'
               AND message_id IN (SELECT id FROM messages WHERE kind = 'reply')",
            [],
            |row| row.get(0),
        )
        .expect("herdr count");
    assert_eq!(herdr_prompts, 0);
    let delivery = read_json(&mut inbox.reader);
    assert_eq!(delivery["method"], "inbox.delivery");
    assert_eq!(delivery["params"]["kind"], "reply");
    assert_eq!(
        Store::open(&database)
            .expect("open")
            .obligation_state(ask)
            .expect("persist"),
        ObligationState::Open
    );
    serde_json::to_writer(
        &mut inbox.stream,
        &serde_json::json!({
            "id": "ack-1",
            "method": "inbox.ack",
            "params": {"message_id": delivery["params"]["message_id"]},
        }),
    )
    .expect("ack");
    inbox.stream.write_all(b"\n").expect("nl");
    let ack = read_json(&mut inbox.reader);
    assert_eq!(ack["result"]["outcome"], "accepted");
    assert_eq!(
        Store::open(&database)
            .expect("open")
            .obligation_state(ask)
            .expect("resolved"),
        ObligationState::Resolved
    );
    daemon.kill().expect("stop");
    daemon.wait().expect("reap");
}

#[test]
fn occupant_ask_envelope_uses_waiter_from_and_reply_to() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let mut store = Store::open(&database).expect("store");
    let waiter = store
        .register_socket_waiter("inbox", Parent::Parentless, "envelope-waiter")
        .expect("waiter");
    let owing = seed_ready_owing(&mut store);
    drop(store);
    let (parsed_tx, parsed_rx) = mpsc::channel();
    let _herdr = spawn_prompt_herdr(&herdr_socket, parsed_tx);
    let (mut daemon, _fault) = boot(&database, &kelpie_socket, &herdr_socket, &fault_socket);
    let asked = send_request(
        &kelpie_socket,
        &serde_json::json!({
            "id": "ask-1",
            "method": "ask",
            "params": {
                "sender": waiter.logical_agent_id,
                "recipient": owing.logical_agent_id,
                "recipient_incarnation": owing.incarnation_id,
                "body": "please answer",
                "idempotency_key": "envelope-ask",
                "from_operator": true
            }
        }),
    );
    assert!(asked["error"].is_null(), "{asked}");
    let prompt = parsed_rx.recv().expect("prompt");
    assert_eq!(prompt["method"], "agent.prompt");
    let envelope = prompt["params"]["text"].as_str().expect("text");
    let ask_id = asked["result"]["message_id"].as_str().expect("ask id");
    assert!(
        envelope.starts_with("<kelpie from=inbox msg="),
        "envelope: {envelope}"
    );
    assert!(
        envelope.contains(&format!("reply-to={ask_id}")),
        "envelope: {envelope}"
    );
    assert!(!envelope.contains("from=operator"), "envelope: {envelope}");
    daemon.kill().expect("stop");
    daemon.wait().expect("reap");
}

#[test]
fn dropped_host_leaves_obligation_open_and_occupant_is_reminded() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let mut store = Store::open(&database).expect("store");
    let waiter = store
        .register_socket_waiter("inbox", Parent::Parentless, "remind-waiter")
        .expect("waiter");
    let owing = seed_ready_owing(&mut store);
    let ask = store
        .create_ask_with_schedule(
            waiter.logical_agent_id,
            owing.logical_agent_id,
            owing.incarnation_id,
            "need a final reply",
            "remind-ask",
            None,
            Some(1),
            true,
        )
        .expect("ask");
    store
        .begin_attempt(ask.operation_id, owing.incarnation_id, "ask-request")
        .expect("attempt");
    store
        .mark_submitted(ask.operation_id, 1, "ask-request")
        .expect("submitted");
    store
        .accept_delivery(
            ask.operation_id,
            owing.incarnation_id,
            "w1:p2",
            "term-owing",
        )
        .expect("accepted");
    store
        .create_reply(
            ask.message_id,
            owing.logical_agent_id,
            "done",
            kelpie::domain::ReplyDisposition::Final,
            "queued-final",
        )
        .expect("queue final");
    drop(store);
    thread::sleep(Duration::from_millis(3));
    let (parsed_tx, parsed_rx) = mpsc::channel();
    let _herdr = spawn_prompt_herdr(&herdr_socket, parsed_tx);
    let (mut daemon, _fault) = boot(&database, &kelpie_socket, &herdr_socket, &fault_socket);
    let prompt = parsed_rx.recv().expect("reminder");
    assert_eq!(prompt["method"], "agent.prompt");
    let envelope = prompt["params"]["text"].as_str().expect("text");
    assert!(
        envelope.starts_with("<kelpie-reminder waiting=inbox"),
        "envelope: {envelope}"
    );
    assert!(
        envelope.contains(&format!("reply-to={}", ask.message_id)),
        "envelope: {envelope}"
    );
    assert_eq!(
        Store::open(&database)
            .expect("open")
            .obligation_state(ask.message_id)
            .expect("open"),
        ObligationState::Open
    );
    daemon.kill().expect("stop");
    daemon.wait().expect("reap");
}

#[test]
fn wrong_sender_cannot_reply_to_socket_waiter_ask() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (waiter, _owing, ask) = seed_waiter_and_ask(&database);
    let _herdr = spawn_startup_herdr(&herdr_socket);
    let (mut daemon, _fault) = boot(&database, &kelpie_socket, &herdr_socket, &fault_socket);
    let refused = send_request(
        &kelpie_socket,
        &serde_json::json!({
            "id": "wrong-reply",
            "method": "reply",
            "params": {
                "reply_to": ask,
                "requester_agent_id": waiter.logical_agent_id,
                "body": "I asked this",
                "disposition": "final",
                "idempotency_key": "wrong-sender"
            }
        }),
    );
    assert_eq!(refused["error"]["class"], "conflict");
    assert!(
        refused["error"]["message"]
            .as_str()
            .expect("msg")
            .contains("only the owing agent can reply"),
        "{refused}"
    );
    assert_eq!(
        Store::open(&database)
            .expect("open")
            .obligation_state(ask)
            .expect("state"),
        ObligationState::Open
    );
    let queued: i64 = Connection::open(&database)
        .expect("db")
        .query_row(
            "SELECT COUNT(*) FROM deliveries WHERE delivery_transport = 'socket_inbox'",
            [],
            |row| row.get(0),
        )
        .expect("count");
    assert_eq!(queued, 0);
    daemon.kill().expect("stop");
    daemon.wait().expect("reap");
}

#[test]
fn cancel_reaches_socket_inbox_and_is_not_resolved() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (waiter, _owing, ask) = seed_waiter_and_ask(&database);
    let _herdr = spawn_startup_herdr(&herdr_socket);
    let (mut daemon, _fault) = boot(&database, &kelpie_socket, &herdr_socket, &fault_socket);
    let cancelled = send_request(
        &kelpie_socket,
        &serde_json::json!({
            "id": "cancel-1",
            "method": "cancel",
            "params": {
                "requester_agent_id": waiter.logical_agent_id,
                "ask_message_id": ask,
                "reason": "obsolete"
            }
        }),
    );
    assert_eq!(cancelled["result"]["state"], "cancelled");
    let mut inbox = claim_inbox(&kelpie_socket, waiter.logical_agent_id, "claim-cancel");
    let delivery = read_json(&mut inbox.reader);
    assert_eq!(delivery["method"], "inbox.delivery");
    assert_eq!(delivery["params"]["kind"], "cancellation");
    assert!(
        delivery["params"]["body"]
            .as_str()
            .expect("body")
            .contains("obsolete")
    );
    let sender: Option<String> = Connection::open(&database)
        .expect("db")
        .query_row(
            "SELECT sender_agent_id FROM messages WHERE kind = 'cancellation'",
            [],
            |row| row.get(0),
        )
        .expect("sender");
    assert_eq!(sender, None);
    assert_eq!(
        Store::open(&database)
            .expect("open")
            .obligation_state(ask)
            .expect("state"),
        ObligationState::Cancelled
    );
    daemon.kill().expect("stop");
    daemon.wait().expect("reap");
}

#[test]
fn reconnect_drains_one_waiter_and_refuses_another() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (waiter, owing, ask) = seed_waiter_and_ask(&database);
    let other = Store::open(&database)
        .expect("store")
        .register_socket_waiter("other", Parent::Parentless, "other-waiter")
        .expect("other");
    let _herdr = spawn_startup_herdr(&herdr_socket);
    let (mut daemon, _fault) = boot(&database, &kelpie_socket, &herdr_socket, &fault_socket);
    let replied = send_request(
        &kelpie_socket,
        &serde_json::json!({
            "id": "reply-1",
            "method": "reply",
            "params": {
                "reply_to": ask,
                "requester_agent_id": owing.logical_agent_id,
                "body": "later reply body",
                "disposition": "final",
                "idempotency_key": "reconnect-final"
            }
        }),
    );
    assert_eq!(replied["result"]["delivery_outcome"], "queued");
    drop(claim_inbox(
        &kelpie_socket,
        waiter.logical_agent_id,
        "claim-1",
    ));
    let other_claim = send_request(
        &kelpie_socket,
        &serde_json::json!({
            "id": "claim-other",
            "method": "inbox.claim",
            "params": {"logical_agent_id": other.logical_agent_id}
        }),
    );
    assert!(other_claim["error"].is_null(), "{other_claim}");
    let mut again = claim_inbox(&kelpie_socket, waiter.logical_agent_id, "claim-2");
    let delivery = read_json(&mut again.reader);
    assert_eq!(delivery["params"]["body"], "later reply body");
    serde_json::to_writer(
        &mut again.stream,
        &serde_json::json!({
            "id": "ack-1",
            "method": "inbox.ack",
            "params": {"message_id": delivery["params"]["message_id"]},
        }),
    )
    .expect("ack");
    again.stream.write_all(b"\n").expect("nl");
    let ack = read_json(&mut again.reader);
    assert_eq!(ack["result"]["outcome"], "accepted");
    let foreign = send_request(
        &kelpie_socket,
        &serde_json::json!({
            "id": "claim-gone",
            "method": "inbox.claim",
            "params": {"logical_agent_id": LogicalAgentId::new()}
        }),
    );
    assert_eq!(foreign["error"]["class"], "conflict");
    daemon.kill().expect("stop");
    daemon.wait().expect("reap");
}
