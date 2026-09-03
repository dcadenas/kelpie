//! Deterministic combined Kelpie and Herdr restart coverage for open asks.

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::thread;
use std::time::Duration;

use kelpie::daemon::Daemon;
use kelpie::domain::{
    InitialMessageIntent, InitialMessageKind, ObligationState, Parent, ReplyDisposition,
    StartIntent,
};
use kelpie::herdr::{AgentObservation, HerdrClient};
use kelpie::slice::Kelpie;
use kelpie::store::Store;
use serde_json::Value;

fn intent(name: &str, terminal: &str, key: &str) -> StartIntent {
    StartIntent {
        public_name: name.into(),
        logical_agent_id: None,
        parent: Parent::Parentless,
        herdr_session: "restart-test".into(),
        pane_id: "w1:p1".into(),
        expected_terminal_id: terminal.into(),
        backend_kind: "codex".into(),
        backend_args: vec![],
        initial_message: InitialMessageIntent {
            sender: None,
            kind: InitialMessageKind::Tell,
            body: "work".into(),
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

fn owing_agent() -> AgentObservation {
    AgentObservation {
        terminal_id: "term-owing".into(),
        pane_id: "w1:p1".into(),
        name: Some("owing".into()),
        agent: Some("codex".into()),
        interactive_ready: true,
        launch_pending: false,
        agent_session: None,
    }
}

fn waiting_agent() -> AgentObservation {
    AgentObservation {
        terminal_id: "term-waiting".into(),
        pane_id: "w1:p2".into(),
        name: Some("waiting".into()),
        agent: Some("codex".into()),
        interactive_ready: true,
        launch_pending: false,
        agent_session: None,
    }
}

fn spawn_herdr_generation(socket: &Path, expect_prompt: bool) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind fake Herdr generation");
    thread::spawn(move || {
        let agents = serde_json::json!([owing_agent(), waiting_agent()]);
        let responses = [
            (
                "ping",
                serde_json::json!({"type":"pong","version":"test","protocol":20}),
            ),
            (
                "session.snapshot",
                serde_json::json!({
                    "type":"session_snapshot",
                    "snapshot":{"protocol":20,"panes":[],"agents":agents}
                }),
            ),
        ];
        for (expected_method, result) in responses {
            let (mut stream, _) = listener.accept().expect("accept Herdr request");
            let mut line = String::new();
            BufReader::new(stream.try_clone().expect("clone stream"))
                .read_line(&mut line)
                .expect("read Herdr request");
            let request: Value = serde_json::from_str(&line).expect("request JSON");
            assert_eq!(request["method"], expected_method);
            serde_json::to_writer(
                &mut stream,
                &serde_json::json!({"id":request["id"],"result":result}),
            )
            .expect("write response");
            stream.write_all(b"\n").expect("finish response");
        }
        if expect_prompt {
            let (mut stream, _) = listener.accept().expect("accept reply prompt");
            let mut line = String::new();
            BufReader::new(stream.try_clone().expect("clone stream"))
                .read_line(&mut line)
                .expect("read prompt");
            let request: Value = serde_json::from_str(&line).expect("prompt JSON");
            assert_eq!(request["method"], "agent.prompt");
            assert_eq!(request["params"]["target"], "w1:p2");
            let text = request["params"]["text"].as_str().expect("text");
            assert!(text.contains(" final>"));
            assert!(text.contains("re="));
            serde_json::to_writer(
                &mut stream,
                &serde_json::json!({
                    "id": request["id"],
                    "result": {
                        "type": "agent_prompted",
                        "agent": waiting_agent()
                    }
                }),
            )
            .expect("write prompt response");
            stream.write_all(b"\n").expect("finish prompt");
        }
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

fn recovered_daemon(database: &Path, kelpie_socket: &Path, herdr_socket: &Path) -> Daemon {
    let store = Store::open(database).expect("open durable store");
    let herdr = HerdrClient::new(herdr_socket, Duration::from_secs(1));
    let mut kelpie = Kelpie::new(store, herdr);
    kelpie.recover().expect("startup recovery");
    Daemon::bind(kelpie_socket, kelpie).expect("bind Kelpie daemon")
}

#[test]
fn open_ask_survives_kelpie_and_herdr_restart_then_resolves() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let (waiting, owing, ask) = {
        let mut store = Store::open(&database).expect("store");
        let mut waiting_intent = intent("waiting", "term-waiting", "waiting-start");
        waiting_intent.pane_id = "w1:p2".into();
        let waiting = store.declare_start(&waiting_intent).expect("waiting");
        let owing = store
            .declare_start(&intent("owing", "term-owing", "owing-start"))
            .expect("owing");
        store
            .begin_attempt(owing.operation_id, owing.incarnation_id, "owing-request")
            .expect("owing attempt");
        store
            .accept_start_ready(
                owing.operation_id,
                owing.incarnation_id,
                &owing_agent(),
                None,
            )
            .expect("owing ready");
        store
            .begin_attempt(
                waiting.operation_id,
                waiting.incarnation_id,
                "waiting-request",
            )
            .expect("waiting attempt");
        store
            .accept_start_ready(
                waiting.operation_id,
                waiting.incarnation_id,
                &waiting_agent(),
                None,
            )
            .expect("waiting ready");
        let ask = store
            .create_ask(
                waiting.logical_agent_id,
                owing.logical_agent_id,
                owing.incarnation_id,
                "survive both restarts",
                "restart-ask",
            )
            .expect("ask");
        (waiting, owing, ask)
    };

    let first_herdr = spawn_herdr_generation(&herdr_socket, false);
    let mut first_daemon = recovered_daemon(&database, &kelpie_socket, &herdr_socket);
    let first_server = thread::spawn(move || first_daemon.serve_one().expect("serve pending"));
    let pending = send_request(
        &kelpie_socket,
        &serde_json::json!({
            "id":"pending-before-restart",
            "method":"pending",
            "params":{"agent_id":owing.logical_agent_id}
        }),
    );
    assert_eq!(
        pending["result"].as_array().expect("pending array").len(),
        1
    );
    first_server.join().expect("first daemon generation");
    first_herdr.join().expect("first Herdr generation");
    fs::remove_file(&herdr_socket).expect("remove stopped Herdr socket");

    let second_herdr = spawn_herdr_generation(&herdr_socket, true);
    let mut second_daemon = recovered_daemon(&database, &kelpie_socket, &herdr_socket);
    let second_server = thread::spawn(move || second_daemon.serve_one().expect("serve reply"));
    let reply = send_request(
        &kelpie_socket,
        &serde_json::json!({
            "id":"reply-after-restart",
            "method":"reply",
            "params":{
                "reply_to":ask.message_id,
                "requester_agent_id":owing.logical_agent_id,
                "body":"resolved after restart",
                "disposition":ReplyDisposition::Final,
                "idempotency_key":"restart-final"
            }
        }),
    );
    assert!(reply["error"].is_null(), "reply should succeed: {reply}");
    assert_eq!(reply["result"]["delivery_outcome"], "accepted");
    assert_eq!(reply["result"]["obligation_state"], "resolved");
    assert_eq!(
        reply["result"]["recipient_incarnation"],
        waiting.incarnation_id.to_string()
    );
    second_server.join().expect("second daemon generation");
    second_herdr.join().expect("second Herdr generation");

    let reopened = Store::open(&database).expect("reopen store");
    assert_eq!(
        reopened
            .obligation_state(ask.message_id)
            .expect("obligation"),
        ObligationState::Resolved
    );
}
