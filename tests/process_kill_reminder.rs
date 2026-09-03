//! Real-`kelpied` process-kill coverage: a queued final must not write a reminder.

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

use kelpie::domain::{
    InitialMessageIntent, InitialMessageKind, ObligationState, Parent, ReplyDisposition,
    StartIntent,
};
use kelpie::herdr::AgentObservation;
use kelpie::store::Store;
use rusqlite::Connection;
use serde_json::Value;

const DAEMON_BOUND: &str = "daemon_bound";

fn intent(name: &str, pane: &str, terminal: &str, key: &str) -> StartIntent {
    StartIntent {
        public_name: name.into(),
        logical_agent_id: None,
        parent: Parent::Parentless,
        herdr_session: "reminder-fault-test".into(),
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

fn seed_queued_final(database: &Path) -> kelpie::domain::MessageId {
    let mut store = Store::open(database).expect("open seed store");
    let waiter = store
        .register_socket_waiter("inbox", Parent::Parentless, "seed-waiter")
        .expect("waiter");
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
    let ask = store
        .create_ask_with_schedule(
            waiter.logical_agent_id,
            owing.logical_agent_id,
            owing.incarnation_id,
            "need a final reply",
            "seed-ask",
            None,
            Some(1),
            false,
        )
        .expect("ask");
    store
        .begin_attempt(ask.operation_id, owing.incarnation_id, "ask-request")
        .expect("ask attempt");
    store
        .mark_submitted(ask.operation_id, 1, "ask-request")
        .expect("ask submitted");
    store
        .accept_delivery(
            ask.operation_id,
            owing.incarnation_id,
            "w1:p2",
            "term-owing",
        )
        .expect("ask accepted");
    store
        .create_reply(
            ask.message_id,
            owing.logical_agent_id,
            "done",
            ReplyDisposition::Final,
            "queued-final",
        )
        .expect("queue final");
    ask.message_id
}

fn spawn_herdr(socket: &Path) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("bind fake Herdr");
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
                    panic!("reminder agent.prompt crossed with a queued final present")
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

fn reminder_attempts(database: &Path) -> i64 {
    Connection::open(database)
        .expect("open state")
        .query_row("SELECT COUNT(*) FROM reminder_attempts", [], |row| {
            row.get(0)
        })
        .expect("count")
}

fn obligation_state(database: &Path, ask: kelpie::domain::MessageId) -> ObligationState {
    Store::open(database)
        .expect("open")
        .obligation_state(ask)
        .expect("state")
}

#[test]
fn kill_with_queued_final_recovers_without_reminder_write() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let ask = seed_queued_final(&database);
    thread::sleep(Duration::from_millis(3));
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault");
    let _herdr = spawn_herdr(&herdr_socket);
    let mut first_daemon = spawn_kelpied(&database, &kelpie_socket, &herdr_socket, &fault_socket);
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release");
    let recover = send_request(
        &kelpie_socket,
        &serde_json::json!({"id":"recover-1","method":"recover","params":{}}),
    );
    assert!(recover.get("result").is_some(), "{recover}");
    assert_eq!(reminder_attempts(&database), 0);
    assert_eq!(obligation_state(&database, ask), ObligationState::Open);
    first_daemon.kill().expect("kill first kelpied");
    first_daemon.wait().expect("reap first kelpied");

    fs::remove_file(&kelpie_socket).expect("remove killed daemon socket");
    let mut recovered = spawn_kelpied(&database, &kelpie_socket, &herdr_socket, &fault_socket);
    let mut recovered_bound = accept_point(&fault_listener, DAEMON_BOUND);
    recovered_bound.write_all(b"x").expect("release recovered");
    let recover_again = send_request(
        &kelpie_socket,
        &serde_json::json!({"id":"recover-2","method":"recover","params":{}}),
    );
    assert!(recover_again.get("result").is_some(), "{recover_again}");
    assert_eq!(reminder_attempts(&database), 0);
    assert_eq!(obligation_state(&database, ask), ObligationState::Open);
    recovered.kill().expect("kill recovered");
    recovered.wait().expect("reap recovered");
}
