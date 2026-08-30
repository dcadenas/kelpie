use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::thread;
use std::time::Duration;

use kelpie::domain::{
    InitialMessageIntent, InitialMessageKind, ObligationState, Parent, StartIntent,
};
use kelpie::herdr::{AgentObservation, HerdrClient};
use kelpie::slice::Kelpie;
use kelpie::store::{Store, store_clock_ms};

fn intent(name: &str, pane: &str, terminal: &str, key: &str) -> StartIntent {
    StartIntent {
        public_name: name.into(),
        logical_agent_id: None,
        parent: Parent::Parentless,
        herdr_session: "test".into(),
        pane_id: pane.into(),
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
        .begin_attempt(
            declared.operation_id,
            declared.incarnation_id,
            &format!("{key}:start"),
        )
        .expect("attempt");
    store
        .accept_start_ready(
            declared.operation_id,
            declared.incarnation_id,
            &AgentObservation {
                terminal_id: terminal.into(),
                pane_id: pane.into(),
                name: Some(name.into()),
                agent: Some("codex".into()),
                interactive_ready: true,
                launch_pending: false,
                agent_session: None,
            },
            None,
        )
        .expect("ready");
    declared
}

fn accepted_reminder_ask(
    store: &mut Store,
    interval_ms: i64,
) -> (kelpie::store::CreatedAsk, kelpie::store::DeclaredStart) {
    let waiting = ready(store, "waiting", "w:p1", "term-1", "waiting-start");
    let owing = ready(store, "owing", "w:p2", "term-2", "owing-start");
    let ask = store
        .create_ask_with_schedule(
            waiting.logical_agent_id,
            owing.logical_agent_id,
            owing.incarnation_id,
            "question",
            "reminder-ask",
            None,
            Some(interval_ms),
        )
        .expect("ask");
    store
        .begin_attempt(ask.operation_id, owing.incarnation_id, "ask-request")
        .expect("attempt");
    store
        .mark_submitted(ask.operation_id, 1, "ask-request")
        .expect("submitted");
    store
        .accept_delivery(ask.operation_id, owing.incarnation_id, "w:p2", "term-2")
        .expect("accepted");
    (ask, owing)
}

#[test]
fn reminder_arms_only_after_ask_acceptance_and_survives_restart() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let mut store = Store::open(&database).expect("store");
    let (ask, owing) = accepted_reminder_ask(&mut store, 1);
    thread::sleep(Duration::from_millis(3));
    drop(store);

    let reopened = Store::open(&database).expect("reopen");
    let due = reopened
        .due_reminders(store_clock_ms().expect("clock"))
        .expect("due");
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].ask_message_id, ask.message_id);
    assert_eq!(due[0].recipient_incarnation, owing.incarnation_id);
}

#[test]
fn unknown_attempt_suspends_retries_without_resolving_obligation() {
    let mut store = Store::in_memory().expect("store");
    let (ask, owing) = accepted_reminder_ask(&mut store, 1);
    thread::sleep(Duration::from_millis(3));
    let now = store_clock_ms().expect("clock");
    let reminder = store.due_reminders(now).expect("due").remove(0);
    store
        .prepare_reminder_attempt(&reminder, "reminder-request", now)
        .expect("prepare");
    store
        .submit_reminder_attempt("reminder-request")
        .expect("submit");
    store
        .resolve_reminder_attempt("reminder-request", "unknown", Some("disconnect"), now)
        .expect("resolve unknown");

    assert!(
        store
            .due_reminders(now + 10_000)
            .expect("due after")
            .is_empty()
    );
    assert_eq!(
        store.obligation_state(ask.message_id).expect("state"),
        ObligationState::Open
    );
    assert_eq!(reminder.recipient_incarnation, owing.incarnation_id);
}

#[test]
fn disable_stops_reminders_without_resolving_obligation() {
    let mut store = Store::in_memory().expect("store");
    let (ask, owing) = accepted_reminder_ask(&mut store, 1);
    store
        .disable_reminder(owing.logical_agent_id, ask.message_id)
        .expect("disable");

    assert!(
        store
            .due_reminders(store_clock_ms().expect("clock") + 10_000)
            .expect("due")
            .is_empty()
    );
    assert_eq!(
        store.obligation_state(ask.message_id).expect("state"),
        ObligationState::Open
    );
}

#[test]
fn restart_suspends_submitted_reminder_without_retry() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("kelpie.sqlite3");
    let mut store = Store::open(&database).expect("store");
    let (ask, _) = accepted_reminder_ask(&mut store, 1);
    thread::sleep(Duration::from_millis(3));
    let now = store_clock_ms().expect("clock");
    let reminder = store.due_reminders(now).expect("due").remove(0);
    store
        .prepare_reminder_attempt(&reminder, "restart-reminder", now)
        .expect("prepare");
    store
        .submit_reminder_attempt("restart-reminder")
        .expect("submit");
    drop(store);

    let mut reopened = Store::open(&database).expect("reopen");
    assert_eq!(reopened.reconcile_reminder_attempts().expect("recover"), 1);
    assert!(
        reopened
            .due_reminders(now + 10_000)
            .expect("due")
            .is_empty()
    );
    assert_eq!(
        reopened.obligation_state(ask.message_id).expect("state"),
        ObligationState::Open
    );
}

#[test]
fn idle_exact_incarnation_receives_correlated_reminder() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("herdr.sock");
    let listener = UnixListener::bind(&socket).expect("bind");
    let server = thread::spawn(move || {
        let (mut snapshot_stream, _) = listener.accept().expect("snapshot");
        let mut line = String::new();
        BufReader::new(snapshot_stream.try_clone().expect("clone"))
            .read_line(&mut line)
            .expect("read snapshot");
        let request: serde_json::Value = serde_json::from_str(&line).expect("snapshot json");
        assert_eq!(request["method"], "session.snapshot");
        serde_json::to_writer(
            &mut snapshot_stream,
            &serde_json::json!({
                "id": request["id"],
                "result": {
                    "type": "session_snapshot",
                    "snapshot": {
                        "protocol": 20,
                        "panes": [],
                        "agents": [{
                            "terminal_id": "term-2",
                            "pane_id": "w:p2",
                            "name": "owing",
                            "agent": "codex",
                            "agent_status": "idle",
                            "interactive_ready": true,
                            "launch_pending": false
                        }]
                    }
                }
            }),
        )
        .expect("write snapshot");
        snapshot_stream.write_all(b"\n").expect("finish snapshot");

        let (mut prompt_stream, _) = listener.accept().expect("prompt");
        line.clear();
        BufReader::new(prompt_stream.try_clone().expect("clone"))
            .read_line(&mut line)
            .expect("read prompt");
        let request: serde_json::Value = serde_json::from_str(&line).expect("prompt json");
        assert_eq!(request["method"], "agent.prompt");
        assert_eq!(request["params"]["target"], "w:p2");
        assert!(
            request["params"]["text"]
                .as_str()
                .expect("text")
                .contains("Pending final reply")
        );
        serde_json::to_writer(
            &mut prompt_stream,
            &serde_json::json!({
                "id": request["id"],
                "result": {
                    "type": "agent_prompted",
                    "agent": {
                        "terminal_id": "term-2",
                        "pane_id": "w:p2",
                        "name": "owing",
                        "agent": "codex",
                        "interactive_ready": true,
                        "launch_pending": false
                    }
                }
            }),
        )
        .expect("write prompt");
        prompt_stream.write_all(b"\n").expect("finish prompt");
    });

    let mut store = Store::in_memory().expect("store");
    accepted_reminder_ask(&mut store, 1);
    thread::sleep(Duration::from_millis(3));
    let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_secs(1)));
    assert_eq!(kelpie.fire_due_reminders().expect("fire"), 1);
    server.join().expect("server");
}

#[test]
fn first_working_to_stopped_boundary_is_eligible_before_timeout() {
    let mut store = Store::in_memory().expect("store");
    let (ask, _) = accepted_reminder_ask(&mut store, 300_000);
    let now = store_clock_ms().expect("clock");
    let boundary = store
        .boundary_reminders(now)
        .expect("initial boundary")
        .remove(0);
    assert!(!boundary.saw_working);
    assert!(store.due_reminders(now).expect("not timed out").is_empty());

    store
        .observe_reminder_lifecycle(ask.message_id, true, now)
        .expect("observe working");
    let stopped = store
        .boundary_reminders(now)
        .expect("stopped boundary")
        .remove(0);
    assert!(stopped.saw_working);
    store
        .prepare_reminder_attempt(&stopped.reminder, "boundary-reminder", now)
        .expect("boundary can remind before timeout");
}
