//! Real-`kelpied` pull reply-sink conformance with a fake Herdr server.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use kelpie::domain::{
    InitialMessageIntent, InitialMessageKind, LogicalAgentId, MessageId, Parent, ReplyDelivery,
    StartIntent,
};
use kelpie::herdr::{AgentObservation, HerdrClient};
use kelpie::slice::Kelpie;
use kelpie::store::{DeclaredStart, Store};
use rusqlite::Connection;
use serde_json::Value;

const DAEMON_BOUND: &str = "daemon_bound";
const ASK_PULL_AFTER_PERSIST: &str = "ask_pull_after_persist";

fn intent(name: &str, pane: &str, terminal: &str, key: &str) -> StartIntent {
    StartIntent {
        public_name: name.into(),
        logical_agent_id: None,
        parent: Parent::Parentless,
        herdr_session: "ask-pull".into(),
        pane_id: pane.into(),
        expected_terminal_id: terminal.into(),
        backend_kind: "codex".into(),
        backend_args: vec![],
        initial_message: InitialMessageIntent {
            sender: None,
            kind: InitialMessageKind::Tell,
            body: "seed only".into(),
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

fn agents() -> Value {
    serde_json::json!([
        observation("parent", "w1:p1", "term-parent"),
        observation("reviewer", "w1:p2", "term-reviewer")
    ])
}

fn seed_ready_pair(database: &Path) -> (DeclaredStart, DeclaredStart) {
    let mut store = Store::open(database).expect("store");
    let parent = store
        .declare_start(&intent("parent", "w1:p1", "term-parent", "parent-start"))
        .expect("declare parent");
    let reviewer = store
        .declare_start(&intent(
            "reviewer",
            "w1:p2",
            "term-reviewer",
            "reviewer-start",
        ))
        .expect("declare reviewer");
    for (declared, agent) in [
        (parent, observation("parent", "w1:p1", "term-parent")),
        (reviewer, observation("reviewer", "w1:p2", "term-reviewer")),
    ] {
        store
            .begin_attempt(declared.operation_id, declared.incarnation_id, "seed-start")
            .expect("begin");
        store
            .accept_start_ready(declared.operation_id, declared.incarnation_id, &agent, None)
            .expect("ready");
    }
    (parent, reviewer)
}

fn spawn_prompt_herdr(socket: &Path, log: Arc<Mutex<Vec<Value>>>) -> thread::JoinHandle<()> {
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
                    "snapshot":{"protocol":20,"panes":[],"agents":agents()}
                }),
                Some("agent.prompt") => {
                    log.lock().expect("log").push(request.clone());
                    serde_json::json!({
                        "type": "agent_prompted",
                        "agent": observation("reviewer", "w1:p2", "term-reviewer")
                    })
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

fn boot(
    database: &Path,
    kelpie_socket: &Path,
    herdr_socket: &Path,
    fault_socket: &Path,
) -> (Child, UnixListener) {
    let fault_listener = UnixListener::bind(fault_socket).expect("bind fault");
    let daemon = spawn_kelpied(
        database,
        kelpie_socket,
        herdr_socket,
        fault_socket,
        DAEMON_BOUND,
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release");
    (daemon, fault_listener)
}

fn pull_ask(
    kelpie_socket: &Path,
    parent: LogicalAgentId,
    reviewer: LogicalAgentId,
    incarnation: kelpie::domain::IncarnationId,
    key: &str,
) -> MessageId {
    let response = send_request(
        kelpie_socket,
        &serde_json::json!({
            "id": key,
            "method": "ask",
            "params": {
                "sender": parent,
                "recipient": reviewer,
                "recipient_incarnation": incarnation,
                "body": "review this",
                "idempotency_key": key,
                "no_remind": true,
                "reply_delivery": "pull"
            }
        }),
    );
    assert!(response["error"].is_null(), "{response}");
    assert_eq!(response["result"]["reply_delivery"], "pull");
    serde_json::from_value(response["result"]["message_id"].clone()).expect("ask id")
}

fn reply(
    kelpie_socket: &Path,
    ask: MessageId,
    owing: LogicalAgentId,
    disposition: &str,
    body: &str,
    key: &str,
) -> Value {
    send_request(
        kelpie_socket,
        &serde_json::json!({
            "id": key,
            "method": "reply",
            "params": {
                "reply_to": ask,
                "requester_agent_id": owing,
                "body": body,
                "disposition": disposition,
                "idempotency_key": key
            }
        }),
    )
}

fn claim(kelpie_socket: &Path, ask: MessageId, waiter: LogicalAgentId, id: &str) -> i64 {
    let response = send_request(
        kelpie_socket,
        &serde_json::json!({
            "id": id,
            "method": "replies.claim",
            "params": {
                "ask_message_id": ask,
                "requester_agent_id": waiter
            }
        }),
    );
    assert!(response["error"].is_null(), "{response}");
    response["result"]["lease_id"].as_i64().expect("lease")
}

fn poll(
    kelpie_socket: &Path,
    ask: MessageId,
    waiter: LogicalAgentId,
    after: i64,
    lease: Option<i64>,
    timeout_ms: Option<i64>,
    id: &str,
) -> Value {
    let mut params = serde_json::json!({
        "ask_message_id": ask,
        "requester_agent_id": waiter,
        "after": after,
    });
    if let Some(lease) = lease {
        params["lease_id"] = serde_json::json!(lease);
    }
    if let Some(timeout_ms) = timeout_ms {
        params["timeout_ms"] = serde_json::json!(timeout_ms);
    }
    send_request(
        kelpie_socket,
        &serde_json::json!({
            "id": id,
            "method": "replies",
            "params": params
        }),
    )
}

fn ack(
    kelpie_socket: &Path,
    ask: MessageId,
    waiter: LogicalAgentId,
    message: &Value,
    lease: i64,
    id: &str,
) -> Value {
    send_request(
        kelpie_socket,
        &serde_json::json!({
            "id": id,
            "method": "replies.ack",
            "params": {
                "ask_message_id": ask,
                "requester_agent_id": waiter,
                "message_id": message,
                "lease_id": lease
            }
        }),
    )
}

fn parent_prompt_count(log: &Arc<Mutex<Vec<Value>>>) -> usize {
    log.lock()
        .expect("log")
        .iter()
        .filter(|request| {
            request["method"] == "agent.prompt" && request["params"]["pane_id"] == "w1:p1"
        })
        .count()
}

fn herdr_reply_deliveries(database: &Path) -> i64 {
    Connection::open(database)
        .expect("db")
        .query_row(
            "SELECT COUNT(*) FROM deliveries
              WHERE delivery_transport = 'herdr_prompt'
                AND message_id IN (
                    SELECT id FROM messages WHERE kind IN ('reply','cancellation')
                )
                AND recipient_incarnation_id IN (
                    SELECT id FROM incarnations WHERE observed_pane_id = 'w1:p1'
                )",
            [],
            |row| row.get(0),
        )
        .expect("count")
}

fn obligation_state(database: &Path, ask: MessageId) -> String {
    Connection::open(database)
        .expect("db")
        .query_row(
            "SELECT state FROM obligations WHERE ask_message_id = ?1",
            [ask.to_string()],
            |row| row.get(0),
        )
        .expect("state")
}

#[test]
#[allow(clippy::too_many_lines)]
fn pull_progress_and_final_skip_parent_pane() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (parent, reviewer) = seed_ready_pair(&database);
    let log = Arc::new(Mutex::new(Vec::new()));
    let _herdr = spawn_prompt_herdr(&herdr_socket, Arc::clone(&log));
    let (mut daemon, _fault) = boot(&database, &kelpie_socket, &herdr_socket, &fault_socket);

    let ask = pull_ask(
        &kelpie_socket,
        parent.logical_agent_id,
        reviewer.logical_agent_id,
        reviewer.incarnation_id,
        "pull-ask",
    );
    let after_ask = parent_prompt_count(&log);

    let progress = reply(
        &kelpie_socket,
        ask,
        reviewer.logical_agent_id,
        "progress",
        "working",
        "pull-progress",
    );
    assert_eq!(progress["result"]["delivery_outcome"], "queued");
    assert_eq!(progress["result"]["obligation_state"], "in_progress");
    assert!(progress["result"]["recipient_incarnation"].is_null());
    assert_eq!(obligation_state(&database, ask), "in_progress");

    let lease = claim(&kelpie_socket, ask, parent.logical_agent_id, "claim-1");
    let polled = poll(
        &kelpie_socket,
        ask,
        parent.logical_agent_id,
        0,
        Some(lease),
        None,
        "poll-1",
    );
    assert_eq!(polled["result"]["status"], "ok");
    assert_eq!(
        polled["result"]["events"].as_array().expect("events").len(),
        1
    );
    assert_eq!(polled["result"]["events"][0]["body"], "working");
    let progress_id = polled["result"]["events"][0]["message_id"].clone();
    let acked = ack(
        &kelpie_socket,
        ask,
        parent.logical_agent_id,
        &progress_id,
        lease,
        "ack-progress",
    );
    assert_eq!(acked["result"]["outcome"], "accepted");
    assert_eq!(acked["result"]["obligation_state"], "in_progress");

    let final_reply = reply(
        &kelpie_socket,
        ask,
        reviewer.logical_agent_id,
        "final",
        "done",
        "pull-final",
    );
    assert_eq!(final_reply["result"]["delivery_outcome"], "queued");
    assert_eq!(final_reply["result"]["obligation_state"], "in_progress");
    assert_eq!(obligation_state(&database, ask), "in_progress");

    let again = poll(
        &kelpie_socket,
        ask,
        parent.logical_agent_id,
        0,
        Some(lease),
        None,
        "poll-2",
    );
    assert_eq!(
        again["result"]["events"].as_array().expect("events").len(),
        2
    );
    let same = poll(
        &kelpie_socket,
        ask,
        parent.logical_agent_id,
        0,
        Some(lease),
        None,
        "poll-2b",
    );
    assert_eq!(
        same["result"]["events"].as_array().expect("events").len(),
        2
    );
    let final_id = again["result"]["events"][1]["message_id"].clone();
    let resolved = ack(
        &kelpie_socket,
        ask,
        parent.logical_agent_id,
        &final_id,
        lease,
        "ack-final",
    );
    assert_eq!(resolved["result"]["outcome"], "accepted");
    assert_eq!(resolved["result"]["obligation_state"], "resolved");
    let second = ack(
        &kelpie_socket,
        ask,
        parent.logical_agent_id,
        &final_id,
        lease,
        "ack-final-2",
    );
    assert_eq!(second["result"]["outcome"], "accepted");
    assert_eq!(second["result"]["obligation_state"], "resolved");

    assert_eq!(parent_prompt_count(&log), after_ask);
    assert_eq!(herdr_reply_deliveries(&database), 0);
    daemon.kill().ok();
}

#[test]
fn empty_poll_is_pending_and_timeout_is_success() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (parent, reviewer) = seed_ready_pair(&database);
    let log = Arc::new(Mutex::new(Vec::new()));
    let _herdr = spawn_prompt_herdr(&herdr_socket, Arc::clone(&log));
    let (mut daemon, _fault) = boot(&database, &kelpie_socket, &herdr_socket, &fault_socket);
    let ask = pull_ask(
        &kelpie_socket,
        parent.logical_agent_id,
        reviewer.logical_agent_id,
        reviewer.incarnation_id,
        "empty-ask",
    );
    let empty = poll(
        &kelpie_socket,
        ask,
        parent.logical_agent_id,
        0,
        None,
        None,
        "empty",
    );
    assert!(empty["error"].is_null(), "{empty}");
    assert_eq!(empty["result"]["status"], "pending");
    assert_eq!(empty["result"]["cursor"], 0);
    assert_eq!(
        empty["result"]["events"].as_array().expect("events").len(),
        0
    );

    let started = Instant::now();
    let timed = poll(
        &kelpie_socket,
        ask,
        parent.logical_agent_id,
        0,
        None,
        Some(200),
        "timeout",
    );
    assert!(timed["error"].is_null(), "{timed}");
    assert_eq!(timed["result"]["status"], "pending");
    assert_eq!(timed["result"]["cursor"], 0);
    assert!(started.elapsed() >= Duration::from_millis(150));

    let refused = poll(
        &kelpie_socket,
        ask,
        parent.logical_agent_id,
        0,
        None,
        Some(61_000),
        "too-long",
    );
    assert_eq!(refused["error"]["class"], "invalid_request");
    daemon.kill().ok();
}

#[test]
fn non_owner_and_stale_lease_are_conflict() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (parent, reviewer) = seed_ready_pair(&database);
    let log = Arc::new(Mutex::new(Vec::new()));
    let _herdr = spawn_prompt_herdr(&herdr_socket, Arc::clone(&log));
    let (mut daemon, _fault) = boot(&database, &kelpie_socket, &herdr_socket, &fault_socket);
    let ask = pull_ask(
        &kelpie_socket,
        parent.logical_agent_id,
        reviewer.logical_agent_id,
        reviewer.incarnation_id,
        "owner-ask",
    );
    let _ = reply(
        &kelpie_socket,
        ask,
        reviewer.logical_agent_id,
        "final",
        "done",
        "owner-final",
    );
    let foreign = poll(
        &kelpie_socket,
        ask,
        reviewer.logical_agent_id,
        0,
        None,
        None,
        "foreign-poll",
    );
    assert_eq!(foreign["error"]["class"], "conflict");
    let old = claim(&kelpie_socket, ask, parent.logical_agent_id, "lease-old");
    let fresh = claim(&kelpie_socket, ask, parent.logical_agent_id, "lease-new");
    assert_ne!(old, fresh);
    let polled = poll(
        &kelpie_socket,
        ask,
        parent.logical_agent_id,
        0,
        Some(fresh),
        None,
        "poll-fresh",
    );
    let message_id = polled["result"]["events"][0]["message_id"].clone();
    let stale = ack(
        &kelpie_socket,
        ask,
        parent.logical_agent_id,
        &message_id,
        old,
        "stale-ack",
    );
    assert_eq!(stale["error"]["class"], "conflict");
    assert_eq!(obligation_state(&database, ask), "open");
    let ok = ack(
        &kelpie_socket,
        ask,
        parent.logical_agent_id,
        &message_id,
        fresh,
        "fresh-ack",
    );
    assert_eq!(ok["result"]["obligation_state"], "resolved");
    daemon.kill().ok();
}

#[test]
fn socket_inbox_and_policy_replay_refuse_wrong_pull() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (_parent, reviewer) = seed_ready_pair(&database);
    let log = Arc::new(Mutex::new(Vec::new()));
    let _herdr = spawn_prompt_herdr(&herdr_socket, Arc::clone(&log));
    let (mut daemon, _fault) = boot(&database, &kelpie_socket, &herdr_socket, &fault_socket);

    let waiter = {
        let mut store = Store::open(&database).expect("store");
        store
            .register_socket_waiter("inbox", Parent::Parentless, "seed-waiter")
            .expect("waiter")
    };
    let refused = send_request(
        &kelpie_socket,
        &serde_json::json!({
            "id": "socket-pull",
            "method": "ask",
            "params": {
                "sender": waiter.logical_agent_id,
                "recipient": reviewer.logical_agent_id,
                "recipient_incarnation": reviewer.incarnation_id,
                "body": "no",
                "idempotency_key": "socket-pull",
                "no_remind": true,
                "reply_delivery": "pull"
            }
        }),
    );
    assert_eq!(refused["error"]["class"], "conflict");
    daemon.kill().ok();
}

#[test]
fn ask_replay_keeps_policy_and_refuses_a_different_one() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (parent, reviewer) = seed_ready_pair(&database);
    let log = Arc::new(Mutex::new(Vec::new()));
    let _herdr = spawn_prompt_herdr(&herdr_socket, Arc::clone(&log));
    let (mut daemon, _fault) = boot(&database, &kelpie_socket, &herdr_socket, &fault_socket);
    let first = pull_ask(
        &kelpie_socket,
        parent.logical_agent_id,
        reviewer.logical_agent_id,
        reviewer.incarnation_id,
        "same-key",
    );
    let replay = send_request(
        &kelpie_socket,
        &serde_json::json!({
            "id": "replay-same",
            "method": "ask",
            "params": {
                "sender": parent.logical_agent_id,
                "recipient": reviewer.logical_agent_id,
                "recipient_incarnation": reviewer.incarnation_id,
                "body": "review this",
                "idempotency_key": "same-key",
                "no_remind": true,
                "reply_delivery": "pull"
            }
        }),
    );
    assert!(replay["error"].is_null(), "{replay}");
    assert_eq!(replay["result"]["message_id"], serde_json::json!(first));
    let different = send_request(
        &kelpie_socket,
        &serde_json::json!({
            "id": "replay-diff",
            "method": "ask",
            "params": {
                "sender": parent.logical_agent_id,
                "recipient": reviewer.logical_agent_id,
                "recipient_incarnation": reviewer.incarnation_id,
                "body": "review this",
                "idempotency_key": "same-key",
                "no_remind": true,
                "reply_delivery": "inject"
            }
        }),
    );
    assert_eq!(different["error"]["class"], "conflict");
    daemon.kill().ok();
}

#[test]
fn waiting_side_cancel_is_a_sink_event() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (parent, reviewer) = seed_ready_pair(&database);
    let log = Arc::new(Mutex::new(Vec::new()));
    let _herdr = spawn_prompt_herdr(&herdr_socket, Arc::clone(&log));
    let (mut daemon, _fault) = boot(&database, &kelpie_socket, &herdr_socket, &fault_socket);
    let ask = pull_ask(
        &kelpie_socket,
        parent.logical_agent_id,
        reviewer.logical_agent_id,
        reviewer.incarnation_id,
        "cancel-ask",
    );
    let after_ask = parent_prompt_count(&log);
    let cancelled = send_request(
        &kelpie_socket,
        &serde_json::json!({
            "id": "cancel",
            "method": "cancel",
            "params": {
                "requester_agent_id": parent.logical_agent_id,
                "ask_message_id": ask,
                "reason": "no longer needed"
            }
        }),
    );
    assert!(cancelled["error"].is_null(), "{cancelled}");
    assert_eq!(parent_prompt_count(&log), after_ask);
    let lease = claim(&kelpie_socket, ask, parent.logical_agent_id, "cancel-claim");
    let polled = poll(
        &kelpie_socket,
        ask,
        parent.logical_agent_id,
        0,
        Some(lease),
        None,
        "cancel-poll",
    );
    assert_eq!(polled["result"]["events"][0]["kind"], "cancellation");
    assert_eq!(herdr_reply_deliveries(&database), 0);
    daemon.kill().ok();
}

#[test]
fn long_poll_returns_on_first_event() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (parent, reviewer) = seed_ready_pair(&database);
    let log = Arc::new(Mutex::new(Vec::new()));
    let _herdr = spawn_prompt_herdr(&herdr_socket, Arc::clone(&log));
    let (mut daemon, _fault) = boot(&database, &kelpie_socket, &herdr_socket, &fault_socket);
    let ask = pull_ask(
        &kelpie_socket,
        parent.logical_agent_id,
        reviewer.logical_agent_id,
        reviewer.incarnation_id,
        "wait-ask",
    );
    let waiter = parent.logical_agent_id;
    let owing = reviewer.logical_agent_id;
    let socket = kelpie_socket.clone();
    let handle =
        thread::spawn(move || poll(&socket, ask, waiter, 0, None, Some(5_000), "long-poll"));
    thread::sleep(Duration::from_millis(150));
    let _ = reply(
        &kelpie_socket,
        ask,
        owing,
        "progress",
        "hello",
        "wait-progress",
    );
    let response = handle.join().expect("join");
    assert!(response["error"].is_null(), "{response}");
    assert_eq!(response["result"]["status"], "ok");
    assert_eq!(response["result"]["events"][0]["body"], "hello");
    daemon.kill().ok();
}

#[test]
fn dropped_poller_does_not_inject_and_replacement_recovers() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (parent, reviewer) = seed_ready_pair(&database);
    let log = Arc::new(Mutex::new(Vec::new()));
    let _herdr = spawn_prompt_herdr(&herdr_socket, Arc::clone(&log));
    let (mut daemon, _fault) = boot(&database, &kelpie_socket, &herdr_socket, &fault_socket);
    let ask = pull_ask(
        &kelpie_socket,
        parent.logical_agent_id,
        reviewer.logical_agent_id,
        reviewer.incarnation_id,
        "drop-ask",
    );
    let after_ask = parent_prompt_count(&log);
    let mut hanging = UnixStream::connect(&kelpie_socket).expect("connect poller");
    serde_json::to_writer(
        &mut hanging,
        &serde_json::json!({
            "id": "hang",
            "method": "replies",
            "params": {
                "ask_message_id": ask,
                "requester_agent_id": parent.logical_agent_id,
                "after": 0,
                "timeout_ms": 5000
            }
        }),
    )
    .expect("write hang");
    hanging.write_all(b"\n").expect("nl");
    drop(hanging);
    let _ = reply(
        &kelpie_socket,
        ask,
        reviewer.logical_agent_id,
        "final",
        "done",
        "drop-final",
    );
    assert_eq!(parent_prompt_count(&log), after_ask);
    assert_eq!(obligation_state(&database, ask), "open");
    let lease = claim(
        &kelpie_socket,
        ask,
        parent.logical_agent_id,
        "recover-claim",
    );
    let polled = poll(
        &kelpie_socket,
        ask,
        parent.logical_agent_id,
        0,
        Some(lease),
        None,
        "recover-poll",
    );
    let message_id = polled["result"]["events"][0]["message_id"].clone();
    let recovered = ack(
        &kelpie_socket,
        ask,
        parent.logical_agent_id,
        &message_id,
        lease,
        "recover-ack",
    );
    assert_eq!(recovered["result"]["obligation_state"], "resolved");
    assert_eq!(parent_prompt_count(&log), after_ask);
    daemon.kill().ok();
}

#[test]
fn cross_ask_ack_is_conflict_and_does_not_brick_the_named_obligation() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (parent, reviewer) = seed_ready_pair(&database);
    let log = Arc::new(Mutex::new(Vec::new()));
    let _herdr = spawn_prompt_herdr(&herdr_socket, Arc::clone(&log));
    let (mut daemon, _fault) = boot(&database, &kelpie_socket, &herdr_socket, &fault_socket);
    let ask_a = pull_ask(
        &kelpie_socket,
        parent.logical_agent_id,
        reviewer.logical_agent_id,
        reviewer.incarnation_id,
        "ask-a",
    );
    let ask_b = pull_ask(
        &kelpie_socket,
        parent.logical_agent_id,
        reviewer.logical_agent_id,
        reviewer.incarnation_id,
        "ask-b",
    );
    let final_a = reply(
        &kelpie_socket,
        ask_a,
        reviewer.logical_agent_id,
        "final",
        "done-a",
        "final-a",
    );
    let message_a = final_a["result"]["message_id"].clone();
    let lease_b = claim(&kelpie_socket, ask_b, parent.logical_agent_id, "lease-b");
    let crossed = ack(
        &kelpie_socket,
        ask_b,
        parent.logical_agent_id,
        &message_a,
        lease_b,
        "cross-ack",
    );
    assert_eq!(crossed["error"]["class"], "conflict", "{crossed}");
    assert_eq!(obligation_state(&database, ask_a), "open");
    assert_eq!(obligation_state(&database, ask_b), "open");

    let lease_a = claim(&kelpie_socket, ask_a, parent.logical_agent_id, "lease-a");
    let resolved = ack(
        &kelpie_socket,
        ask_a,
        parent.logical_agent_id,
        &message_a,
        lease_a,
        "ack-a",
    );
    assert_eq!(resolved["result"]["outcome"], "accepted");
    assert_eq!(resolved["result"]["obligation_state"], "resolved");
    assert_eq!(obligation_state(&database, ask_b), "open");
    assert_eq!(herdr_reply_deliveries(&database), 0);
    daemon.kill().ok();
}

#[test]
#[allow(clippy::too_many_lines)]
fn kill_after_pull_persist_recovers_without_inject() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (parent, reviewer) = seed_ready_pair(&database);
    let log = Arc::new(Mutex::new(Vec::new()));
    let _herdr = spawn_prompt_herdr(&herdr_socket, Arc::clone(&log));
    let fault_listener = UnixListener::bind(&fault_socket).expect("bind fault");
    let mut daemon = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        &format!("{DAEMON_BOUND},{ASK_PULL_AFTER_PERSIST}"),
    );
    let mut bound = accept_point(&fault_listener, DAEMON_BOUND);
    bound.write_all(b"x").expect("release");
    let ask = pull_ask(
        &kelpie_socket,
        parent.logical_agent_id,
        reviewer.logical_agent_id,
        reviewer.incarnation_id,
        "kill-ask",
    );
    let after_ask = parent_prompt_count(&log);
    let socket = kelpie_socket.clone();
    let owing = reviewer.logical_agent_id;
    let client = thread::spawn(move || {
        let mut stream = UnixStream::connect(socket).expect("connect reply");
        serde_json::to_writer(
            &mut stream,
            &serde_json::json!({
                "id": "kill-final",
                "method": "reply",
                "params": {
                    "reply_to": ask,
                    "requester_agent_id": owing,
                    "body": "done",
                    "disposition": "final",
                    "idempotency_key": "kill-final"
                }
            }),
        )
        .expect("write reply");
        stream.write_all(b"\n").expect("nl");
        let mut response = Vec::new();
        stream.read_to_end(&mut response).expect("until death");
        response
    });
    let persisted = accept_point(&fault_listener, ASK_PULL_AFTER_PERSIST);
    daemon.kill().expect("kill kelpied");
    daemon.wait().expect("reap");
    drop(persisted);
    assert!(client.join().expect("reply client").is_empty());
    assert_eq!(obligation_state(&database, ask), "open");
    assert_eq!(herdr_reply_deliveries(&database), 0);
    assert_eq!(parent_prompt_count(&log), after_ask);

    fs::remove_file(&kelpie_socket).expect("remove killed socket");
    let mut recovered = spawn_kelpied(
        &database,
        &kelpie_socket,
        &herdr_socket,
        &fault_socket,
        DAEMON_BOUND,
    );
    let mut recovered_bound = accept_point(&fault_listener, DAEMON_BOUND);
    recovered_bound.write_all(b"x").expect("release recovered");
    assert_eq!(obligation_state(&database, ask), "open");
    assert_eq!(herdr_reply_deliveries(&database), 0);
    let lease = claim(
        &kelpie_socket,
        ask,
        parent.logical_agent_id,
        "recover-lease",
    );
    let polled = poll(
        &kelpie_socket,
        ask,
        parent.logical_agent_id,
        0,
        Some(lease),
        None,
        "recover-poll",
    );
    assert_eq!(polled["result"]["status"], "ok");
    let message_id = polled["result"]["events"][0]["message_id"].clone();
    let acked = ack(
        &kelpie_socket,
        ask,
        parent.logical_agent_id,
        &message_id,
        lease,
        "recover-ack",
    );
    assert_eq!(acked["result"]["obligation_state"], "resolved");
    assert_eq!(parent_prompt_count(&log), after_ask);
    assert_eq!(herdr_reply_deliveries(&database), 0);
    recovered.kill().ok();
    recovered.wait().ok();
}

#[test]
fn start_ask_pull_copies_policy_and_skips_parent_pane() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let herdr_socket = directory.path().join("herdr.sock");
    let fault_socket = directory.path().join("fault.sock");
    let (parent, reviewer) = seed_ready_pair(&database);
    let mut start = intent("reviewer", "w1:p2", "term-reviewer", "start-pull-initial");
    start.initial_message = InitialMessageIntent {
        sender: Some(parent.logical_agent_id),
        kind: InitialMessageKind::Ask,
        body: "review via start".into(),
        reply_delivery: ReplyDelivery::Inject,
    };
    start.reply_delivery = ReplyDelivery::Pull;
    let mut kelpie = Kelpie::new(
        Store::open(&database).expect("store"),
        HerdrClient::new("/unused", Duration::from_secs(1)),
    );
    let (_prepared, ask) = kelpie
        .begin_initial_message(&start, reviewer)
        .expect("start ask");
    let policy: String = Connection::open(&database)
        .expect("db")
        .query_row(
            "SELECT reply_delivery FROM obligations WHERE ask_message_id = ?1",
            [ask.to_string()],
            |row| row.get(0),
        )
        .expect("policy");
    assert_eq!(policy, "pull");

    let log = Arc::new(Mutex::new(Vec::new()));
    let _herdr = spawn_prompt_herdr(&herdr_socket, Arc::clone(&log));
    let (mut daemon, _fault) = boot(&database, &kelpie_socket, &herdr_socket, &fault_socket);
    let after_boot = parent_prompt_count(&log);
    let _ = reply(
        &kelpie_socket,
        ask,
        reviewer.logical_agent_id,
        "final",
        "done",
        "start-final",
    );
    assert_eq!(parent_prompt_count(&log), after_boot);
    assert_eq!(herdr_reply_deliveries(&database), 0);
    let lease = claim(&kelpie_socket, ask, parent.logical_agent_id, "start-claim");
    let polled = poll(
        &kelpie_socket,
        ask,
        parent.logical_agent_id,
        0,
        Some(lease),
        None,
        "start-poll",
    );
    let message_id = polled["result"]["events"][0]["message_id"].clone();
    let acked = ack(
        &kelpie_socket,
        ask,
        parent.logical_agent_id,
        &message_id,
        lease,
        "start-ack",
    );
    assert_eq!(acked["result"]["obligation_state"], "resolved");
    daemon.kill().ok();
}
