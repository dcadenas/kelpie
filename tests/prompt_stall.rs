//! A prompt Herdr wrote but observed no activity for is accepted evidence,
//! surfaced to the caller, and never treated as a rejection.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::thread;
use std::time::Duration;

use kelpie::daemon::Daemon;
use kelpie::domain::{
    InitialMessageIntent, InitialMessageKind, Parent, ReplyDelivery, StartIntent,
};
use kelpie::herdr::{AgentObservation, HerdrClient};
use kelpie::slice::Kelpie;
use kelpie::store::Store;
use rusqlite::Connection;
use serde_json::Value;

const STALL: &str = "agent_prompt_stalled";

fn intent(name: &str, pane: &str, terminal: &str, key: &str) -> StartIntent {
    StartIntent {
        public_name: name.into(),
        logical_agent_id: None,
        parent: Parent::Parentless,
        herdr_session: "prompt-stall-test".into(),
        pane_id: pane.into(),
        expected_terminal_id: terminal.into(),
        backend_kind: "codex".into(),
        backend_args: vec![],
        initial_message: InitialMessageIntent {
            sender: None,
            kind: InitialMessageKind::Tell,
            body: "work".into(),
            reply_delivery: ReplyDelivery::Inject,
        },
        working_directory: "/tmp/work".into(),
        idempotency_key: key.into(),
        readiness_timeout_ms: 5_000,
        keep_open: true,
        supersedes: None,
        requested_model: None,
        requested_provider: None,
        requested_effort: None,
        reply_delivery: ReplyDelivery::Inject,
    }
}

fn observation(pane: &str, terminal: &str, name: &str) -> AgentObservation {
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

fn ready(
    store: &mut Store,
    name: &str,
    pane: &str,
    terminal: &str,
    key: &str,
) -> kelpie::store::DeclaredStart {
    let declared = store
        .declare_start(&intent(name, pane, terminal, key))
        .expect("declare");
    store
        .begin_attempt(declared.operation_id, declared.incarnation_id, key)
        .expect("attempt");
    store
        .accept_start_ready(
            declared.operation_id,
            declared.incarnation_id,
            &observation(pane, terminal, name),
            None,
        )
        .expect("ready");
    declared
}

/// Answer the one prompt the delivery path writes with a stall error.
fn spawn_herdr(socket: &Path) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind fake Herdr");
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept prompt");
        let mut line = String::new();
        BufReader::new(stream.try_clone().expect("clone stream"))
            .read_line(&mut line)
            .expect("read request");
        let request: Value = serde_json::from_str(&line).expect("request JSON");
        assert_eq!(request["method"], "agent.prompt");
        assert_eq!(
            request["params"]["wait"]["until"],
            serde_json::json!(["working", "blocked"])
        );
        let response = serde_json::json!({
            "id": request["id"],
            "error": {"code": STALL, "message": "no activity observed"}
        });
        serde_json::to_writer(&mut stream, &response).expect("response");
        stream.write_all(b"\n").expect("finish response");
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

#[test]
fn stalled_ask_response_carries_submission_and_keeps_the_obligation_open() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");

    let (waiting, owing) = {
        let mut store = Store::open(&database).expect("store");
        let waiting = ready(
            &mut store,
            "waiting",
            "w:p1",
            "term-waiting",
            "waiting-start",
        );
        let owing = ready(&mut store, "owing", "w:p2", "term-owing", "owing-start");
        (waiting, owing)
    };

    let herdr = spawn_herdr(&herdr_socket);
    let store = Store::open(&database).expect("open durable store");
    let herdr_client = HerdrClient::new(&herdr_socket, Duration::from_secs(1));
    let mut daemon =
        Daemon::bind(&kelpie_socket, Kelpie::new(store, herdr_client)).expect("bind daemon");
    let server = thread::spawn(move || daemon.serve_one().expect("serve ask"));
    let response = send_request(
        &kelpie_socket,
        &serde_json::json!({
            "id": "stalled-ask",
            "method": "ask",
            "params": {
                "sender": waiting.logical_agent_id,
                "recipient": owing.logical_agent_id,
                "recipient_incarnation": owing.incarnation_id,
                "body": "may not arrive",
                "idempotency_key": "stalled-ask-rpc",
                "from_operator": false
            }
        }),
    );
    server.join().expect("server thread");
    herdr.join().expect("fake Herdr");

    assert!(response["error"].is_null(), "{response}");
    assert_eq!(response["result"]["delivery_outcome"], "accepted");
    assert_eq!(response["result"]["submission"], "stalled");

    let ask_id = response["result"]["message_id"]
        .as_i64()
        .expect("ask message id");
    let connection = Connection::open(&database).expect("reopen database");
    let evidence: String = connection
        .query_row(
            "SELECT evidence_json FROM operation_attempts
             WHERE operation_id = (SELECT operation_id FROM deliveries WHERE message_id = ?1)
             ORDER BY attempt_number DESC LIMIT 1",
            [ask_id],
            |row| row.get(0),
        )
        .expect("attempt evidence");
    assert!(
        evidence.contains("\"submission\":\"stalled\""),
        "{evidence}"
    );
    let notice: String = connection
        .query_row(
            "SELECT body FROM operator_notices ORDER BY created_at_ms DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .expect("operator notice");
    assert!(notice.contains("no observed agent activity"), "{notice}");
    let obligation: String = connection
        .query_row(
            "SELECT state FROM obligations WHERE ask_message_id = ?1",
            [ask_id],
            |row| row.get(0),
        )
        .expect("obligation");
    assert_eq!(obligation, "open");
}
