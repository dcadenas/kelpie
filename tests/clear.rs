//! Standalone backend clear timing and fail-closed behavior.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use kelpie::daemon::Daemon;
use kelpie::domain::{
    InitialMessageIntent, InitialMessageKind, Parent, RenewIntent, RenewTimeout, StartIntent,
};
use kelpie::herdr::{AgentObservation, HerdrClient};
use kelpie::slice::{Kelpie, SliceError};
use kelpie::store::{DeclaredStart, DueReminder, Store, store_clock_ms};

fn intent(backend: &str) -> StartIntent {
    StartIntent {
        public_name: "worker".into(),
        logical_agent_id: None,
        parent: Parent::Parentless,
        herdr_session: "test".into(),
        pane_id: "w:p1".into(),
        expected_terminal_id: "term-1".into(),
        backend_kind: backend.into(),
        backend_args: vec![],
        initial_message: InitialMessageIntent {
            sender: None,
            kind: InitialMessageKind::Tell,
            body: "work".into(),
        },
        working_directory: "/tmp/work".into(),
        idempotency_key: format!("start-{backend}"),
        readiness_timeout_ms: 5_000,
        keep_open: true,
        supersedes: None,
        requested_model: None,
        requested_provider: None,
        requested_effort: None,
    }
}

fn observation(backend: &str, session: &str) -> AgentObservation {
    AgentObservation {
        terminal_id: "term-1".into(),
        pane_id: "w:p1".into(),
        name: Some("worker".into()),
        agent: Some(backend.into()),
        interactive_ready: true,
        launch_pending: false,
        agent_session: Some(serde_json::Value::String(session.into())),
    }
}

fn observation_without_session(backend: &str) -> AgentObservation {
    let mut observed = observation(backend, "unused");
    observed.agent_session = None;
    observed
}

fn ready(store: &mut Store, backend: &str) -> DeclaredStart {
    let declared = store.declare_start(&intent(backend)).expect("declare");
    store
        .begin_attempt(declared.operation_id, declared.incarnation_id, "seed-start")
        .expect("attempt");
    store
        .accept_start_ready(
            declared.operation_id,
            declared.incarnation_id,
            &observation(backend, "sess-1"),
            None,
        )
        .expect("ready");
    declared
}

fn serve(
    listener: UnixListener,
    backend: &'static str,
    sessions: &'static [&'static str],
) -> thread::JoinHandle<Vec<serde_json::Value>> {
    thread::spawn(move || {
        let mut requests = Vec::new();
        for session in sessions {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut line = String::new();
            BufReader::new(stream.try_clone().expect("clone"))
                .read_line(&mut line)
                .expect("request");
            let request: serde_json::Value = serde_json::from_str(&line).expect("json");
            requests.push(request.clone());
            serde_json::to_writer(
                &mut stream,
                &serde_json::json!({
                    "id": request["id"],
                    "result": {
                        "type": "agent_info",
                        "agent": observation(backend, session),
                    }
                }),
            )
            .expect("response");
            stream.write_all(b"\n").expect("finish");
        }
        requests
    })
}

fn request_after_connect(
    socket: &std::path::Path,
    request: &serde_json::Value,
    connected: &mpsc::Sender<()>,
) -> serde_json::Value {
    let mut stream = UnixStream::connect(socket).expect("connect");
    connected.send(()).expect("signal connection");
    serde_json::to_writer(&mut stream, &request).expect("request");
    stream.write_all(b"\n").expect("finish request");
    let mut line = String::new();
    BufReader::new(stream)
        .read_line(&mut line)
        .expect("response");
    serde_json::from_str(&line).expect("response json")
}

fn assert_last_prompt_contains(requests: &[serde_json::Value], expected: &str) {
    let request = requests.last().expect("prompt request");
    assert_eq!(request["method"], "agent.prompt");
    assert!(
        request["params"]["text"]
            .as_str()
            .expect("prompt text")
            .contains(expected)
    );
}

#[test]
fn on_clear_waits_for_session_rotation() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("herdr.sock");
    let listener = UnixListener::bind(&socket).expect("bind");
    let server = serve(listener, "claude", &["sess-1", "sess-1", "sess-2"]);
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "claude");
    let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_secs(1)));

    let result = kelpie
        .clear(
            worker.logical_agent_id,
            worker.incarnation_id,
            "clear-claude",
        )
        .expect("clear");

    assert_eq!(result.outcome, kelpie::domain::OperationOutcome::Succeeded);
    let requests = server.join().expect("server");
    assert_eq!(requests[0]["method"], "agent.get");
    assert_eq!(requests[1]["method"], "agent.prompt");
    assert_eq!(requests[1]["params"]["text"], "/clear");
    assert_eq!(requests[2]["method"], "agent.get");
}

#[test]
fn on_next_prompt_returns_without_waiting_for_rotation() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("herdr.sock");
    let listener = UnixListener::bind(&socket).expect("bind");
    let server = serve(
        listener,
        "opencode",
        &["sess-1", "sess-1", "sess-2", "sess-2"],
    );
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "opencode");
    let tell = store
        .create_tell_with_due(
            worker.logical_agent_id,
            worker.logical_agent_id,
            worker.incarnation_id,
            "first prompt after clear",
            "post-clear-tell",
            Some(store_clock_ms().expect("clock")),
        )
        .expect("queued tell");
    store
        .create_renew(&RenewIntent {
            logical_agent_id: worker.logical_agent_id,
            incarnation_id: worker.incarnation_id,
            requester_agent_id: worker.logical_agent_id,
            prepare_prompt: "prepare after clear".into(),
            resume_prompt: "resume".into(),
            prepare_timeout_ms: 60_000,
            on_timeout: RenewTimeout::Abort,
            every_ms: None,
            scheduled_at_ms: store_clock_ms().expect("clock"),
        })
        .expect("scheduled renew");
    let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_secs(1)));
    kelpie.set_prompt_settle_delay_ms(100);

    kelpie
        .clear(
            worker.logical_agent_id,
            worker.incarnation_id,
            "clear-opencode",
        )
        .expect("clear");
    assert_eq!(
        kelpie
            .store()
            .delivery_outcome(tell.operation_id)
            .expect("outcome"),
        kelpie::domain::DeliveryOutcome::Queued
    );
    assert_eq!(kelpie.fire_due_deliveries().expect("hold tell"), 0);
    assert_eq!(kelpie.drive_renews().expect("hold renew"), 0);
    thread::sleep(Duration::from_millis(110));
    assert_eq!(kelpie.fire_due_deliveries().expect("fire tell"), 1);
    assert_eq!(kelpie.drive_renews().expect("fire renew"), 1);

    let requests = server.join().expect("server");
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[0]["method"], "agent.get");
    assert_eq!(requests[1]["method"], "agent.prompt");
    assert_eq!(requests[1]["params"]["text"], "/clear");
    assert!(requests.iter().any(|request| {
        request["params"]["text"]
            .as_str()
            .is_some_and(|text| text.contains("first prompt after clear"))
    }));
    assert_last_prompt_contains(&requests, "prepare after clear");
}

#[test]
fn on_clear_does_not_treat_a_missing_session_as_rotation() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("herdr.sock");
    let listener = UnixListener::bind(&socket).expect("bind");
    let server = thread::spawn(move || {
        let responses = [
            observation("claude", "sess-1"),
            observation_without_session("claude"),
            observation("claude", "sess-2"),
        ];
        let mut methods = Vec::new();
        for observed in responses {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut line = String::new();
            BufReader::new(stream.try_clone().expect("clone"))
                .read_line(&mut line)
                .expect("request");
            let request: serde_json::Value = serde_json::from_str(&line).expect("json");
            methods.push(request["method"].clone());
            serde_json::to_writer(
                &mut stream,
                &serde_json::json!({
                    "id": request["id"],
                    "result": {"type": "agent_info", "agent": observed},
                }),
            )
            .expect("response");
            stream.write_all(b"\n").expect("finish");
        }
        methods
    });
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "claude");
    let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_secs(1)));

    kelpie
        .clear(
            worker.logical_agent_id,
            worker.incarnation_id,
            "clear-missing-session",
        )
        .expect("clear");

    assert_eq!(
        server.join().expect("server"),
        vec!["agent.get", "agent.prompt", "agent.get"]
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn on_clear_rotation_wait_does_not_block_an_unrelated_client() {
    let directory = tempfile::tempdir().expect("tempdir");
    let herdr_socket = directory.path().join("herdr.sock");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let listener = UnixListener::bind(&herdr_socket).expect("bind Herdr");
    let server = serve(
        listener,
        "claude",
        &[
            "sess-1", "sess-1", "sess-1", "sess-1", "sess-1", "sess-2", "sess-2", "sess-2",
        ],
    );
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "claude");
    let ask = store
        .create_ask(
            worker.logical_agent_id,
            worker.logical_agent_id,
            worker.incarnation_id,
            "question",
            "ask-before-clear",
        )
        .expect("ask");
    let mut kelpie = Kelpie::new(
        store,
        HerdrClient::new(&herdr_socket, Duration::from_secs(1)),
    );
    kelpie.set_prompt_settle_delay_ms(100);
    let mut daemon = Daemon::bind(&kelpie_socket, kelpie).expect("daemon");
    let (clear_tx, clear_rx) = mpsc::channel();
    let (clear_connected_tx, clear_connected_rx) = mpsc::channel();
    let clear_socket = kelpie_socket.clone();
    let clear_client = thread::spawn(move || {
        clear_tx
            .send(request_after_connect(
                &clear_socket,
                &serde_json::json!({
                    "id": "clear",
                    "method": "clear",
                    "params": {
                        "recipient": worker.logical_agent_id,
                        "recipient_incarnation": worker.incarnation_id,
                        "idempotency_key": "nonblocking-clear",
                    }
                }),
                &clear_connected_tx,
            ))
            .expect("send clear response");
    });
    clear_connected_rx.recv().expect("clear connected");
    daemon.poll().expect("accept clear");
    assert!(
        clear_rx.try_recv().is_err(),
        "clear must still await rotation"
    );

    let pending_socket = kelpie_socket.clone();
    let (pending_connected_tx, pending_connected_rx) = mpsc::channel();
    let pending_client = thread::spawn(move || {
        request_after_connect(
            &pending_socket,
            &serde_json::json!({
                "id": "pending",
                "method": "pending",
                "params": {"agent_id": worker.logical_agent_id},
            }),
            &pending_connected_tx,
        )
    });
    pending_connected_rx.recv().expect("pending connected");
    daemon.poll().expect("serve pending while clear waits");
    let pending = pending_client.join().expect("pending client");
    assert_eq!(
        pending["result"].as_array().expect("pending array").len(),
        1
    );
    assert_eq!(
        pending["result"][0]["ask_message_id"],
        ask.message_id.to_string()
    );
    assert!(clear_rx.try_recv().is_err(), "clear must remain parked");
    let tell_socket = kelpie_socket.clone();
    let (tell_connected_tx, tell_connected_rx) = mpsc::channel();
    let tell_client = thread::spawn(move || {
        request_after_connect(
            &tell_socket,
            &serde_json::json!({
                "id": "tell-during-clear",
                "method": "tell",
                "params": {
                    "sender": worker.logical_agent_id,
                    "recipient": worker.logical_agent_id,
                    "recipient_incarnation": worker.incarnation_id,
                    "body": "after clear",
                    "idempotency_key": "tell-during-clear",
                },
            }),
            &tell_connected_tx,
        )
    });
    tell_connected_rx.recv().expect("tell connected");
    daemon.poll().expect("queue tell while clear waits");
    assert_eq!(
        tell_client.join().expect("tell client")["result"]["delivery_outcome"],
        "queued"
    );
    assert!(clear_rx.try_recv().is_err(), "clear must remain parked");

    let reply_socket = kelpie_socket.clone();
    let (reply_connected_tx, reply_connected_rx) = mpsc::channel();
    let reply_client = thread::spawn(move || {
        request_after_connect(
            &reply_socket,
            &serde_json::json!({
                "id": "reply-during-clear",
                "method": "reply",
                "params": {
                    "reply_to": ask.message_id,
                    "requester_agent_id": worker.logical_agent_id,
                    "body": "final answer",
                    "disposition": "final",
                    "idempotency_key": "reply-during-clear",
                },
            }),
            &reply_connected_tx,
        )
    });
    reply_connected_rx.recv().expect("reply connected");
    daemon.poll().expect("queue reply while clear waits");
    let reply_response = reply_client.join().expect("reply client");
    assert_eq!(reply_response["result"]["delivery_outcome"], "queued");
    assert_eq!(reply_response["result"]["obligation_state"], "open");
    assert!(clear_rx.try_recv().is_err(), "clear must remain parked");

    daemon.poll().expect("observe rotation");
    assert_eq!(
        clear_rx.recv().expect("clear response")["result"]["outcome"],
        "succeeded"
    );
    clear_client.join().expect("clear client");
    thread::sleep(Duration::from_millis(110));
    daemon.poll().expect("deliver tell after clear");
    let requests = server.join().expect("server");
    assert!(requests.iter().any(|request| {
        request["params"]["text"]
            .as_str()
            .is_some_and(|text| text.contains("after clear"))
    }));
    assert_last_prompt_contains(&requests, "final answer");
    let pending_socket = kelpie_socket.clone();
    let (connected_tx, connected_rx) = mpsc::channel();
    let pending_client = thread::spawn(move || {
        request_after_connect(
            &pending_socket,
            &serde_json::json!({
                "id": "pending-after-reply",
                "method": "pending",
                "params": {"agent_id": worker.logical_agent_id},
            }),
            &connected_tx,
        )
    });
    connected_rx.recv().expect("pending connected");
    daemon.poll().expect("serve pending after reply");
    assert_eq!(
        pending_client.join().expect("pending client")["result"],
        serde_json::json!([])
    );
}

#[test]
fn standalone_clear_waits_out_the_latest_prompt_settle_gap() {
    let directory = tempfile::tempdir().expect("tempdir");
    let herdr_socket = directory.path().join("herdr.sock");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let listener = UnixListener::bind(&herdr_socket).expect("bind Herdr");
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "opencode");
    let tell = store
        .create_tell(
            worker.logical_agent_id,
            worker.logical_agent_id,
            worker.incarnation_id,
            "previous prompt",
            "previous-prompt",
        )
        .expect("tell");
    let attempt = store
        .begin_attempt(
            tell.operation_id,
            worker.incarnation_id,
            "previous-prompt-request",
        )
        .expect("attempt");
    store
        .mark_submitted(tell.operation_id, attempt, "previous-prompt-request")
        .expect("submitted");
    store
        .mark_unknown(
            tell.operation_id,
            worker.incarnation_id,
            "prompt may have landed",
        )
        .expect("unknown");
    let mut kelpie = Kelpie::new(
        store,
        HerdrClient::new(&herdr_socket, Duration::from_secs(1)),
    );
    kelpie.set_prompt_settle_delay_ms(100);
    let mut daemon = Daemon::bind(&kelpie_socket, kelpie).expect("daemon");
    let (connected_tx, connected_rx) = mpsc::channel();
    let clear_socket = kelpie_socket.clone();
    let client = thread::spawn(move || {
        request_after_connect(
            &clear_socket,
            &serde_json::json!({
                "id": "paced-clear",
                "method": "clear",
                "params": {
                    "recipient": worker.logical_agent_id,
                    "recipient_incarnation": worker.incarnation_id,
                    "idempotency_key": "paced-clear",
                }
            }),
            &connected_tx,
        )
    });
    connected_rx.recv().expect("connected");
    daemon.poll().expect("park clear");
    listener.set_nonblocking(true).expect("nonblocking");
    assert!(
        listener.accept().is_err(),
        "clear must not contact Herdr inside the settle gap"
    );
    listener.set_nonblocking(false).expect("blocking");
    let server = serve(listener, "opencode", &["sess-1", "sess-1"]);
    thread::sleep(Duration::from_millis(110));
    daemon.poll().expect("submit paced clear");
    assert_eq!(
        client.join().expect("client")["result"]["outcome"],
        "succeeded"
    );
    server.join().expect("server");
}

#[test]
fn prompt_spacing_includes_submitted_reminders() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "opencode");
    let ask = store
        .create_ask_with_schedule(
            worker.logical_agent_id,
            worker.logical_agent_id,
            worker.incarnation_id,
            "work",
            "reminded-ask",
            None,
            Some(60_000),
        )
        .expect("ask");
    let attempt = store
        .begin_attempt(ask.operation_id, worker.incarnation_id, "ask-request")
        .expect("attempt");
    store
        .mark_submitted(ask.operation_id, attempt, "ask-request")
        .expect("submitted");
    store
        .accept_delivery(ask.operation_id, worker.incarnation_id, "w:p1", "term-1")
        .expect("accepted");
    let reminder = DueReminder {
        ask_message_id: ask.message_id,
        owing_agent_id: worker.logical_agent_id,
        waiting_agent_id: worker.logical_agent_id,
        recipient_incarnation: worker.incarnation_id,
        pane_id: "w:p1".into(),
        terminal_id: "term-1".into(),
        interval_ms: 60_000,
    };
    let reminder_started_at_ms = store_clock_ms().expect("clock") + 61_000;
    store
        .prepare_reminder_attempt(&reminder, "reminder-request", reminder_started_at_ms)
        .expect("prepare reminder");
    store
        .submit_reminder_attempt("reminder-request")
        .expect("submit reminder");

    assert_eq!(
        store
            .last_prompt_attempt_at_ms(worker.incarnation_id)
            .expect("last prompt"),
        Some(reminder_started_at_ms)
    );
}

#[test]
fn a_due_reminder_waits_out_the_post_clear_gap() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "opencode");
    let ask = store
        .create_ask_with_schedule(
            worker.logical_agent_id,
            worker.logical_agent_id,
            worker.incarnation_id,
            "work",
            "due-reminder-before-clear",
            None,
            Some(1),
        )
        .expect("ask");
    let attempt = store
        .begin_attempt(ask.operation_id, worker.incarnation_id, "ask-request")
        .expect("attempt");
    store
        .mark_submitted(ask.operation_id, attempt, "ask-request")
        .expect("submitted");
    store
        .accept_delivery(ask.operation_id, worker.incarnation_id, "w:p1", "term-1")
        .expect("accepted");
    thread::sleep(Duration::from_millis(2));
    let clear_id = store
        .create_clear(
            worker.logical_agent_id,
            worker.incarnation_id,
            "/clear",
            &serde_json::json!("sess-1"),
            100,
            "clear-before-reminder",
        )
        .expect("clear");
    store
        .complete_clear(clear_id, worker.incarnation_id, None, 100)
        .expect("complete clear");
    let mut kelpie = Kelpie::new(
        store,
        HerdrClient::new("/nonexistent/herdr.sock", Duration::from_millis(50)),
    );
    kelpie.set_prompt_settle_delay_ms(100);

    assert_eq!(kelpie.fire_due_reminders().expect("hold reminder"), 0);
}

#[test]
fn unknown_backend_fails_closed_before_contacting_herdr() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "cursor");
    let mut kelpie = Kelpie::new(
        store,
        HerdrClient::new("/nonexistent/herdr.sock", Duration::from_millis(50)),
    );

    let error = kelpie
        .clear(
            worker.logical_agent_id,
            worker.incarnation_id,
            "clear-cursor",
        )
        .expect_err("cursor is unsupported");

    assert!(matches!(error, SliceError::UnsupportedBackend { .. }));
}

#[test]
fn an_unknown_clear_attempt_still_spaces_the_next_prompt() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "opencode");
    let clear_id = store
        .create_clear(
            worker.logical_agent_id,
            worker.incarnation_id,
            "/clear",
            &serde_json::json!("sess-1"),
            100,
            "unknown-clear",
        )
        .expect("clear intent");
    let attempt = store
        .begin_attempt(clear_id, worker.incarnation_id, "unknown-clear-request")
        .expect("attempt");
    store
        .mark_submitted(clear_id, attempt, "unknown-clear-request")
        .expect("submitted");
    store
        .mark_clear_unknown(clear_id, worker.incarnation_id, "response lost", 100)
        .expect("unknown");
    let mut kelpie = Kelpie::new(
        store,
        HerdrClient::new("/nonexistent/herdr.sock", Duration::from_millis(50)),
    );
    kelpie.set_prompt_settle_delay_ms(100);

    let tell = kelpie
        .tell(
            worker.logical_agent_id,
            worker.logical_agent_id,
            worker.incarnation_id,
            "after ambiguous clear",
            "after-unknown-clear",
            None,
        )
        .expect("queued tell");

    assert_eq!(
        kelpie
            .store()
            .delivery_outcome(tell.operation_id)
            .expect("outcome"),
        kelpie::domain::DeliveryOutcome::Queued
    );
    let report = kelpie
        .store_mut()
        .reconcile(&kelpie::herdr::Snapshot {
            protocol: 20,
            panes: vec![],
            agents: vec![observation("opencode", "sess-1")],
        })
        .expect("recover inside unknown-clear gap");
    assert_eq!(report.outcomes_marked_unknown, 0);
    assert_eq!(
        kelpie
            .store()
            .delivery_outcome(tell.operation_id)
            .expect("preserved outcome"),
        kelpie::domain::DeliveryOutcome::Queued
    );
}

#[test]
fn recovery_preserves_a_delivery_durably_postponed_by_clear() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "opencode");
    let tell = store
        .create_tell_with_due(
            worker.logical_agent_id,
            worker.logical_agent_id,
            worker.incarnation_id,
            "held across recovery",
            "held-across-recovery",
            Some(store_clock_ms().expect("clock")),
        )
        .expect("queued tell");
    let clear_id = store
        .create_clear(
            worker.logical_agent_id,
            worker.incarnation_id,
            "/clear",
            &serde_json::json!("sess-1"),
            100,
            "clear-before-recovery",
        )
        .expect("clear");
    store
        .complete_clear(clear_id, worker.incarnation_id, None, 100)
        .expect("complete clear");

    let report = store
        .reconcile(&kelpie::herdr::Snapshot {
            protocol: 20,
            panes: vec![],
            agents: vec![observation("opencode", "sess-1")],
        })
        .expect("recover inside gap");

    assert_eq!(report.outcomes_marked_unknown, 0);
    assert_eq!(
        store.delivery_outcome(tell.operation_id).expect("outcome"),
        kelpie::domain::DeliveryOutcome::Queued
    );
}

#[test]
fn repeated_recovery_preserves_delivery_after_reconciling_clear_unknown() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "opencode");
    let tell = store
        .create_tell_with_due(
            worker.logical_agent_id,
            worker.logical_agent_id,
            worker.incarnation_id,
            "held across repeated recovery",
            "held-across-repeated-recovery",
            Some(store_clock_ms().expect("clock")),
        )
        .expect("queued tell");
    let clear_id = store
        .create_clear(
            worker.logical_agent_id,
            worker.incarnation_id,
            "/clear",
            &serde_json::json!("sess-1"),
            100,
            "submitted-clear-before-recovery",
        )
        .expect("clear");
    let attempt = store
        .begin_attempt(clear_id, worker.incarnation_id, "submitted-clear")
        .expect("attempt");
    store
        .mark_submitted(clear_id, attempt, "submitted-clear")
        .expect("submitted");
    let snapshot = kelpie::herdr::Snapshot {
        protocol: 20,
        panes: vec![],
        agents: vec![observation("opencode", "sess-1")],
    };

    let first = store.reconcile(&snapshot).expect("first recover");
    assert_eq!(first.outcomes_marked_unknown, 1);
    let notices = store.operator_notices().expect("notices");
    assert_eq!(notices.len(), 1);
    assert!(notices[0].body.contains("clear operation"));
    assert!(notices[0].body.contains("unknown outcome after recovery"));
    let second = store.reconcile(&snapshot).expect("second recover");
    assert_eq!(second.outcomes_marked_unknown, 0);
    assert_eq!(store.operator_notices().expect("notices").len(), 1);
    assert_eq!(
        store.delivery_outcome(tell.operation_id).expect("outcome"),
        kelpie::domain::DeliveryOutcome::Queued
    );
}

#[test]
fn standalone_clear_and_renew_are_mutually_exclusive() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "claude");
    let renew_id = store
        .create_renew(&RenewIntent {
            logical_agent_id: worker.logical_agent_id,
            incarnation_id: worker.incarnation_id,
            requester_agent_id: worker.logical_agent_id,
            prepare_prompt: "prepare".into(),
            resume_prompt: "resume".into(),
            on_timeout: RenewTimeout::Abort,
            prepare_timeout_ms: 60_000,
            every_ms: None,
            scheduled_at_ms: store_clock_ms().expect("clock"),
        })
        .expect("renew");
    store
        .validate_clear_target(worker.logical_agent_id, worker.incarnation_id)
        .expect("a scheduled renew does not block clear");
    let prepare = store
        .create_ask(
            worker.logical_agent_id,
            worker.logical_agent_id,
            worker.incarnation_id,
            "prepare",
            "prepare-for-overlap",
        )
        .expect("prepare ask");
    store
        .mark_renew_preparing(
            renew_id,
            prepare.message_id,
            store_clock_ms().expect("clock") + 60_000,
        )
        .expect("preparing");
    let mut kelpie = Kelpie::new(
        store,
        HerdrClient::new("/nonexistent/herdr.sock", Duration::from_millis(50)),
    );
    assert!(matches!(
        kelpie.clear(
            worker.logical_agent_id,
            worker.incarnation_id,
            "clear-during-renew"
        ),
        Err(SliceError::Store(kelpie::store::StoreError::Conflict(_)))
    ));

    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "claude");
    store
        .create_clear(
            worker.logical_agent_id,
            worker.incarnation_id,
            "/clear",
            &serde_json::Value::String("sess-1".into()),
            100,
            "active-clear",
        )
        .expect("clear intent");
    let mut kelpie = Kelpie::new(
        store,
        HerdrClient::new("/nonexistent/herdr.sock", Duration::from_millis(50)),
    );
    assert!(matches!(
        kelpie.renew(
            worker.logical_agent_id,
            worker.logical_agent_id,
            worker.incarnation_id,
            "prepare",
            "resume",
            RenewTimeout::Abort,
            60_000,
            None,
            store_clock_ms().expect("clock"),
        ),
        Err(SliceError::Store(kelpie::store::StoreError::Conflict(_)))
    ));
}

#[test]
fn scheduled_renew_waits_while_clear_is_in_flight() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "claude");
    let now_ms = store_clock_ms().expect("clock");
    store
        .create_renew(&RenewIntent {
            logical_agent_id: worker.logical_agent_id,
            incarnation_id: worker.incarnation_id,
            requester_agent_id: worker.logical_agent_id,
            prepare_prompt: "prepare".into(),
            resume_prompt: "resume".into(),
            prepare_timeout_ms: 60_000,
            on_timeout: RenewTimeout::Abort,
            every_ms: None,
            scheduled_at_ms: now_ms,
        })
        .expect("renew");
    let clear_id = store
        .create_clear(
            worker.logical_agent_id,
            worker.incarnation_id,
            "/clear",
            &serde_json::json!("sess-1"),
            100,
            "clear-before-scheduled-renew",
        )
        .expect("clear");

    assert!(
        store
            .actionable_renews(now_ms)
            .expect("actionable while clear")
            .is_empty()
    );
    store
        .complete_clear(
            clear_id,
            worker.incarnation_id,
            Some(&serde_json::json!("sess-2")),
            0,
        )
        .expect("complete clear");
    assert_eq!(
        store
            .actionable_renews(now_ms)
            .expect("actionable after clear")
            .len(),
        1
    );
}

#[test]
fn recovery_fails_an_unattempted_clear_without_wedging_the_incarnation() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "claude");
    let clear_id = store
        .create_clear(
            worker.logical_agent_id,
            worker.incarnation_id,
            "/clear",
            &serde_json::json!("sess-1"),
            100,
            "unattempted-clear",
        )
        .expect("clear intent");
    assert!(
        store
            .clear_in_flight(worker.incarnation_id)
            .expect("in flight")
    );
    let snapshot = kelpie::herdr::Snapshot {
        protocol: 20,
        panes: vec![],
        agents: vec![observation("claude", "sess-1")],
    };

    let report = store.reconcile(&snapshot).expect("recover");

    assert_eq!(report.unattempted_clears_failed, 1);
    assert_eq!(
        store.operation_outcome(clear_id).expect("outcome"),
        kelpie::domain::OperationOutcome::Failed
    );
    assert!(
        !store
            .clear_in_flight(worker.incarnation_id)
            .expect("not in flight")
    );
    store
        .validate_clear_target(worker.logical_agent_id, worker.incarnation_id)
        .expect("clear available again");
    assert_eq!(
        store
            .reconcile(&snapshot)
            .expect("idempotent recover")
            .unattempted_clears_failed,
        0
    );
}

/// A clear whose rotation was never observed blocks the next one.
///
/// This is the five-wipe incident of 2026-08-21: a grok occupant whose
/// rotation Herdr could not see took five `/new` commands in seven minutes,
/// one per retry, each destroying a real context to re-ask a question the
/// observation channel was not answering.
#[test]
fn an_unproven_clear_refuses_the_next_one() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("herdr.sock");
    let _listener = UnixListener::bind(&socket).expect("bind");
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "grok");
    let pre_clear = serde_json::json!({"kind": "id", "value": "sess-1"});
    let clear_id = store
        .create_clear(
            worker.logical_agent_id,
            worker.incarnation_id,
            "/new",
            &pre_clear,
            0,
            "clear-one",
        )
        .expect("create clear");
    store
        .mark_clear_unknown(
            clear_id,
            worker.incarnation_id,
            "clear was accepted but session rotation was not observed",
            0,
        )
        .expect("unknown");

    // The durable guard still calls this incarnation clearable: nothing is
    // pending, and `unknown` is not a live operation.
    store
        .validate_clear_target(worker.logical_agent_id, worker.incarnation_id)
        .expect("no clear is in flight");
    // The refusal is the unproven one, and it names the clear to resolve.
    assert_eq!(
        store
            .unproven_clear(worker.incarnation_id)
            .expect("query")
            .map(|id| id.to_string()),
        Some(clear_id.to_string())
    );

    let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_secs(1)));
    let refused = kelpie
        .clear(worker.logical_agent_id, worker.incarnation_id, "clear-two")
        .expect_err("a second clear is refused");
    match refused {
        SliceError::ClearUnproven { operation_id } => {
            assert_eq!(operation_id, clear_id.to_string());
        }
        other => panic!("expected ClearUnproven, got {other:?}"),
    }
}

/// The block lifts on evidence, never on a timer.
///
/// A rotation observed after the unproven clear answers the question that
/// clear left open, so clearing is available again with no operator step.
#[test]
fn an_observed_rotation_releases_the_block() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "grok");
    let pre_clear = serde_json::json!({"kind": "id", "value": "sess-1"});
    let clear_id = store
        .create_clear(
            worker.logical_agent_id,
            worker.incarnation_id,
            "/new",
            &pre_clear,
            0,
            "clear-one",
        )
        .expect("create clear");
    store
        .mark_clear_unknown(clear_id, worker.incarnation_id, "not observed", 0)
        .expect("unknown");
    assert!(
        store
            .unproven_clear(worker.incarnation_id)
            .expect("query")
            .is_some()
    );

    // The clear's resolution and the rotation are ordered by two independent
    // wall-clock reads at millisecond resolution, and "after the clear" is the
    // whole question here. Landing both in one millisecond makes the rotation
    // read as not-after and the assertion a coin flip, so put them in different
    // milliseconds rather than racing the statement timing.
    thread::sleep(Duration::from_millis(2));

    // Reconciliation seeing a different backend-native session is the evidence
    // the clear itself could not produce.
    let rotated = AgentObservation {
        terminal_id: "term-1".into(),
        pane_id: "w:p1".into(),
        name: Some("worker".into()),
        agent: Some("grok".into()),
        interactive_ready: true,
        launch_pending: false,
        agent_session: Some(serde_json::json!({"kind": "id", "value": "sess-2"})),
    };
    store
        .reconcile(&kelpie::herdr::Snapshot {
            protocol: 20,
            panes: vec![],
            agents: vec![rotated],
        })
        .expect("reconcile");

    assert_eq!(
        store.unproven_clear(worker.incarnation_id).expect("query"),
        None,
        "evidence of rotation is what releases the block"
    );
}
