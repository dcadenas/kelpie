use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::thread;
use std::time::Duration;

use kelpie::domain::{
    DeliveryOutcome, InitialMessageIntent, InitialMessageKind, ObligationState, Parent,
    ReplyDisposition, StartIntent,
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
            "ORIGINAL ASK BODY",
            "reminder-ask",
            None,
            Some(interval_ms),
            false,
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
    let (ask, _owing) = accepted_reminder_ask(&mut store, 1);
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
        let text = request["params"]["text"].as_str().expect("text");
        assert!(text.contains("kelpie ask-info"), "{text}");
        assert!(text.contains("kelpie reminder-snooze"), "{text}");
        assert!(text.contains("kelpie reply"), "{text}");
        assert!(text.contains("kelpie cancel"), "{text}");
        assert!(
            !text.contains("ORIGINAL ASK BODY"),
            "ask body was inlined: {text}"
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
fn working_to_idle_never_bypasses_due_time() {
    let mut store = Store::in_memory().expect("store");
    let (ask, _) = accepted_reminder_ask(&mut store, 1_200_000);
    let due = store
        .reminder_info(ask.message_id)
        .unwrap()
        .unwrap()
        .next_eligible_at_ms
        .unwrap();
    store
        .observe_reminder_lifecycle(ask.message_id, true, due - 1)
        .unwrap();
    assert!(store.boundary_reminders(due - 1).unwrap().is_empty());
    assert_eq!(store.next_boundary_check_at_ms().unwrap(), Some(due));
    assert!(store.due_reminders(due - 1).unwrap().is_empty());
    let reminder = store.due_reminders(due).unwrap().remove(0);
    assert!(
        store
            .prepare_reminder_attempt(&reminder, "early", due - 1)
            .is_err()
    );
    store
        .prepare_reminder_attempt(&reminder, "on-time", due)
        .unwrap();
    store.submit_reminder_attempt("on-time").unwrap();
    store
        .resolve_reminder_attempt("on-time", "accepted", None, due)
        .unwrap();
    assert!(store.due_reminders(due + 1_200_000 - 1).unwrap().is_empty());
    assert_eq!(store.due_reminders(due + 1_200_000).unwrap().len(), 1);
}

fn accept_ask(
    store: &mut Store,
    ask: &kelpie::store::CreatedAsk,
    owing: &kelpie::store::DeclaredStart,
    pane: &str,
    terminal: &str,
) {
    store
        .begin_attempt(ask.operation_id, owing.incarnation_id, "ask-request")
        .expect("attempt");
    store
        .mark_submitted(ask.operation_id, 1, "ask-request")
        .expect("submitted");
    store
        .accept_delivery(ask.operation_id, owing.incarnation_id, pane, terminal)
        .expect("accepted");
}

#[test]
fn queued_final_holds_interval_and_boundary_reminders() {
    let mut store = Store::in_memory().expect("store");
    let waiter = store
        .register_socket_waiter("inbox", Parent::Parentless, "hold-waiter")
        .expect("waiter");
    let owing = ready(&mut store, "owing", "w:p2", "term-2", "owing-start");
    let ask = store
        .create_ask_with_schedule(
            waiter.logical_agent_id,
            owing.logical_agent_id,
            owing.incarnation_id,
            "question",
            "queued-final-ask",
            None,
            Some(1),
            false,
        )
        .expect("ask");
    accept_ask(&mut store, &ask, &owing, "w:p2", "term-2");
    store
        .create_reply(
            ask.message_id,
            owing.logical_agent_id,
            "done",
            ReplyDisposition::Final,
            "queued-final",
        )
        .expect("queue final");
    thread::sleep(Duration::from_millis(3));
    let now = store_clock_ms().expect("clock");
    assert!(store.due_reminders(now).expect("due").is_empty());
    assert!(store.boundary_reminders(now).expect("boundary").is_empty());
    assert_eq!(
        store.obligation_state(ask.message_id).expect("state"),
        ObligationState::Open
    );
    let unused = tempfile::tempdir()
        .expect("tempdir")
        .path()
        .join("unused.sock");
    let mut kelpie = Kelpie::new(store, HerdrClient::new(&unused, Duration::from_secs(1)));
    assert_eq!(kelpie.fire_due_reminders().expect("fire"), 0);
}

#[test]
fn submitted_and_unknown_finals_hold_reminders() {
    let mut store = Store::in_memory().expect("store");
    let waiting = ready(&mut store, "waiting", "w:p1", "term-1", "waiting-start");
    let owing = ready(&mut store, "owing", "w:p2", "term-2", "owing-start");
    let ask = store
        .create_ask_with_schedule(
            waiting.logical_agent_id,
            owing.logical_agent_id,
            owing.incarnation_id,
            "question",
            "submitted-final-ask",
            None,
            Some(1),
            false,
        )
        .expect("ask");
    accept_ask(&mut store, &ask, &owing, "w:p2", "term-2");
    let reply = store
        .create_reply(
            ask.message_id,
            owing.logical_agent_id,
            "done",
            ReplyDisposition::Final,
            "submitted-final",
        )
        .expect("create final");
    let operation_id = reply.operation_id.expect("pane operation");
    let incarnation = reply.recipient_incarnation.expect("pane incarnation");
    store
        .begin_attempt(operation_id, incarnation, "final-request")
        .expect("attempt");
    store
        .mark_submitted(operation_id, 1, "final-request")
        .expect("submitted");
    thread::sleep(Duration::from_millis(3));
    let now = store_clock_ms().expect("clock");
    assert!(store.due_reminders(now).expect("due submitted").is_empty());
    store
        .mark_unknown(operation_id, incarnation, "disconnect")
        .expect("unknown");
    let now = store_clock_ms().expect("clock");
    assert!(store.due_reminders(now).expect("due unknown").is_empty());
    assert_eq!(
        store.obligation_state(ask.message_id).expect("state"),
        ObligationState::Open
    );
}

#[test]
fn rejected_final_allows_reminders_again() {
    let mut store = Store::in_memory().expect("store");
    let (ask, owing) = accepted_reminder_ask(&mut store, 1);
    let reply = store
        .create_reply(
            ask.message_id,
            owing.logical_agent_id,
            "done",
            ReplyDisposition::Final,
            "rejected-final",
        )
        .expect("create final");
    let operation_id = reply.operation_id.expect("pane operation");
    let incarnation = reply.recipient_incarnation.expect("pane incarnation");
    store
        .begin_attempt(operation_id, incarnation, "final-request")
        .expect("attempt");
    store
        .mark_submitted(operation_id, 1, "final-request")
        .expect("submitted");
    store
        .mark_rejected(
            operation_id,
            incarnation,
            "herdr refused",
            DeliveryOutcome::Rejected,
        )
        .expect("rejected");
    thread::sleep(Duration::from_millis(3));
    let now = store_clock_ms().expect("clock");
    assert_eq!(store.due_reminders(now).expect("due").len(), 1);
    assert_eq!(
        store.obligation_state(ask.message_id).expect("state"),
        ObligationState::Open
    );
}

#[test]
fn receiver_increase_and_snooze_survive_progress_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("test.sqlite3");
    let mut store = Store::open(&db).unwrap();
    let (ask, owing) = accepted_reminder_ask(&mut store, 1_200_000);
    let wrong = kelpie::domain::LogicalAgentId::try_from(u64::MAX).expect("positive");
    let until = store_clock_ms().unwrap() + 7_200_000;
    assert!(store.snooze_reminder(wrong, ask.message_id, until).is_err());
    assert!(
        store
            .increase_reminder_interval(wrong, ask.message_id, 2_400_000)
            .is_err()
    );
    assert!(
        store
            .increase_reminder_interval(owing.logical_agent_id, ask.message_id, 600_000)
            .is_err()
    );
    store
        .snooze_reminder(owing.logical_agent_id, ask.message_id, until)
        .unwrap();
    store
        .increase_reminder_interval(owing.logical_agent_id, ask.message_id, 2_400_000)
        .unwrap();
    let before = store.reminder_info(ask.message_id).unwrap().unwrap();
    assert_eq!(before.next_eligible_at_ms, Some(until));
    store
        .increase_reminder_interval(owing.logical_agent_id, ask.message_id, 2_400_000)
        .unwrap();
    store
        .create_reply(
            ask.message_id,
            owing.logical_agent_id,
            "started",
            ReplyDisposition::Progress,
            "progress",
        )
        .unwrap();
    drop(store);
    let mut store = Store::open(&db).unwrap();
    let timing = store.ask_info(ask.message_id).unwrap().reminder.unwrap();
    assert_eq!(timing.interval_ms, 2_400_000);
    assert_eq!(timing.snoozed_until_ms, Some(until));
    assert_eq!(timing.next_eligible_at_ms, Some(until));
    assert!(store.due_reminders(until - 1).unwrap().is_empty());
    let reminder = store.due_reminders(until).unwrap().remove(0);
    store
        .prepare_reminder_attempt(&reminder, "after-snooze", until)
        .unwrap();
    store.submit_reminder_attempt("after-snooze").unwrap();
    store
        .resolve_reminder_attempt("after-snooze", "accepted", None, until)
        .unwrap();
    assert!(
        store
            .due_reminders(until + 2_400_000 - 1)
            .unwrap()
            .is_empty()
    );
    assert_eq!(store.due_reminders(until + 2_400_000).unwrap().len(), 1);
}

#[test]
fn in_flight_completion_preserves_new_snooze_and_interval() {
    let mut store = Store::in_memory().unwrap();
    let (ask, owing) = accepted_reminder_ask(&mut store, 1);
    let due = store
        .reminder_info(ask.message_id)
        .unwrap()
        .unwrap()
        .next_eligible_at_ms
        .unwrap();
    let reminder = store.due_reminders(due).unwrap().remove(0);
    store
        .prepare_reminder_attempt(&reminder, "flight", due)
        .unwrap();
    store.submit_reminder_attempt("flight").unwrap();
    let until = store_clock_ms().unwrap() + 7_200_000;
    store
        .snooze_reminder(owing.logical_agent_id, ask.message_id, until)
        .unwrap();
    store
        .increase_reminder_interval(owing.logical_agent_id, ask.message_id, 2_400_000)
        .unwrap();
    store
        .resolve_reminder_attempt("flight", "accepted", None, due)
        .unwrap();
    let timing = store.reminder_info(ask.message_id).unwrap().unwrap();
    assert_eq!(timing.snoozed_until_ms, Some(until));
    assert_eq!(timing.next_eligible_at_ms, Some(until));
    assert!(store.due_reminders(until - 1).unwrap().is_empty());
}

#[test]
fn prepared_reminder_is_rechecked_after_snooze() {
    let mut store = Store::in_memory().unwrap();
    let (ask, owing) = accepted_reminder_ask(&mut store, 1);
    let due = store
        .reminder_info(ask.message_id)
        .unwrap()
        .unwrap()
        .next_eligible_at_ms
        .unwrap();
    let reminder = store.due_reminders(due).unwrap().remove(0);
    store
        .snooze_reminder(
            owing.logical_agent_id,
            ask.message_id,
            store_clock_ms().unwrap() + 7_200_000,
        )
        .unwrap();
    assert!(
        store
            .prepare_reminder_attempt(&reminder, "stale", due)
            .is_err()
    );
}

#[test]
fn disabled_and_terminal_policies_cannot_be_changed() {
    for terminal in [false, true] {
        let mut store = Store::in_memory().unwrap();
        let (ask, owing) = accepted_reminder_ask(&mut store, 1);
        if terminal {
            let reply = store
                .create_reply(
                    ask.message_id,
                    owing.logical_agent_id,
                    "done",
                    ReplyDisposition::Final,
                    "final",
                )
                .unwrap();
            store
                .begin_attempt(
                    reply.operation_id.unwrap(),
                    reply.recipient_incarnation.unwrap(),
                    "reply",
                )
                .unwrap();
            store
                .mark_submitted(reply.operation_id.unwrap(), 1, "reply")
                .unwrap();
            store
                .accept_delivery(
                    reply.operation_id.unwrap(),
                    reply.recipient_incarnation.unwrap(),
                    "w:p1",
                    "term-1",
                )
                .unwrap();
        } else {
            store
                .disable_reminder(owing.logical_agent_id, ask.message_id)
                .unwrap();
        }
        assert!(
            store
                .snooze_reminder(
                    owing.logical_agent_id,
                    ask.message_id,
                    store_clock_ms().unwrap() + 7_200_000
                )
                .is_err()
        );
        assert!(
            store
                .increase_reminder_interval(owing.logical_agent_id, ask.message_id, 2_400_000)
                .is_err()
        );
        assert!(
            store
                .reminder_info(ask.message_id)
                .unwrap()
                .unwrap()
                .next_eligible_at_ms
                .is_none()
        );
        assert!(store.due_reminders(i64::MAX).unwrap().is_empty());
    }
}

#[test]
fn initial_start_ask_arms_forty_five_minute_default_on_acceptance() {
    let mut store = Store::in_memory().unwrap();
    let sender = ready(&mut store, "sender", "w:p1", "term-1", "sender");
    let owing = ready(&mut store, "owing", "w:p2", "term-2", "owing");
    let ask = store
        .create_initial_message(
            owing.logical_agent_id,
            owing.incarnation_id,
            &InitialMessageIntent {
                sender: Some(sender.logical_agent_id),
                kind: InitialMessageKind::Ask,
                body: "work".into(),
            },
            "initial",
        )
        .unwrap();
    let timing = store.reminder_info(ask.message_id).unwrap().unwrap();
    assert_eq!(timing.interval_ms, 2_700_000);
    assert!(timing.next_eligible_at_ms.is_none());
    store
        .begin_attempt(ask.operation_id, owing.incarnation_id, "initial-request")
        .unwrap();
    store
        .mark_submitted(ask.operation_id, 1, "initial-request")
        .unwrap();
    let before = store_clock_ms().unwrap();
    store
        .accept_delivery(ask.operation_id, owing.incarnation_id, "w:p2", "term-2")
        .unwrap();
    let due = store
        .reminder_info(ask.message_id)
        .unwrap()
        .unwrap()
        .next_eligible_at_ms
        .unwrap();
    assert!(due >= before + 2_700_000);
    assert!(store.due_reminders(due - 1).unwrap().is_empty());
}

#[test]
fn migration_is_once_and_preserves_other_policies_and_obligations() {
    for (interval, disabled) in [(300_000, false), (300_000, true), (600_000, false)] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("migration.sqlite3");
        let mut store = Store::open(&db).unwrap();
        let (ask, owing) = accepted_reminder_ask(&mut store, interval);
        if disabled {
            store
                .disable_reminder(owing.logical_agent_id, ask.message_id)
                .unwrap();
        }
        drop(store);
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch("PRAGMA user_version = 28").unwrap();
        let before = store_clock_ms().unwrap();
        drop(conn);
        let store = Store::open(&db).unwrap();
        let timing = store.reminder_info(ask.message_id).unwrap().unwrap();
        assert_eq!(
            timing.interval_ms,
            if interval == 300_000 && !disabled {
                1_200_000
            } else {
                interval
            }
        );
        if interval == 300_000 && !disabled {
            assert!(timing.next_eligible_at_ms.unwrap() >= before + 1_200_000);
        }
        assert_eq!(
            store.obligation_state(ask.message_id).unwrap(),
            ObligationState::Open
        );
        drop(store);
        let store = Store::open(&db).unwrap();
        assert_eq!(
            store
                .reminder_info(ask.message_id)
                .unwrap()
                .unwrap()
                .next_eligible_at_ms,
            timing.next_eligible_at_ms
        );
    }
}

#[test]
fn interval_increase_does_not_arm_an_unaccepted_ask() {
    let mut store = Store::in_memory().unwrap();
    let sender = ready(&mut store, "sender", "w:p1", "term-1", "sender");
    let owing = ready(&mut store, "owing", "w:p2", "term-2", "owing");
    let ask = store
        .create_ask_with_schedule(
            sender.logical_agent_id,
            owing.logical_agent_id,
            owing.incarnation_id,
            "question",
            "unarmed",
            None,
            Some(1_200_000),
            false,
        )
        .unwrap();
    store
        .increase_reminder_interval(owing.logical_agent_id, ask.message_id, 2_400_000)
        .unwrap();
    assert!(
        store
            .reminder_info(ask.message_id)
            .unwrap()
            .unwrap()
            .next_eligible_at_ms
            .is_none()
    );
    assert!(store.due_reminders(i64::MAX).unwrap().is_empty());
}

#[test]
fn migration_retains_later_deadline_and_snooze() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("later.sqlite3");
    let mut store = Store::open(&db).unwrap();
    let (ask, owing) = accepted_reminder_ask(&mut store, 300_000);
    let until = store_clock_ms().unwrap() + 7_200_000;
    store
        .snooze_reminder(owing.logical_agent_id, ask.message_id, until)
        .unwrap();
    drop(store);
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute(
        "UPDATE obligation_reminders SET next_due_at_ms = ?1",
        [until + 1000],
    )
    .unwrap();
    conn.execute_batch("PRAGMA user_version = 28").unwrap();
    drop(conn);
    let store = Store::open(&db).unwrap();
    let timing = store.reminder_info(ask.message_id).unwrap().unwrap();
    assert_eq!(timing.interval_ms, 1_200_000);
    assert_eq!(timing.next_eligible_at_ms, Some(until + 1000));
    assert_eq!(timing.snoozed_until_ms, Some(until));
}

#[test]
fn busy_snapshot_cannot_shorten_a_receiver_interval_increase() {
    let mut store = Store::in_memory().unwrap();
    let (ask, owing) = accepted_reminder_ask(&mut store, 1);
    let due_at = store
        .reminder_info(ask.message_id)
        .unwrap()
        .unwrap()
        .next_eligible_at_ms
        .unwrap();
    let collected = store.due_reminders(due_at).unwrap();
    assert_eq!(collected.len(), 1);
    let mut kelpie = Kelpie::new(store, HerdrClient::new("/unused", Duration::from_secs(1)));
    let before = store_clock_ms().unwrap();
    kelpie
        .increase_reminder_interval(owing.logical_agent_id, ask.message_id, 2_400_000)
        .unwrap();
    let deadline = kelpie
        .store()
        .reminder_info(ask.message_id)
        .unwrap()
        .unwrap()
        .next_eligible_at_ms
        .unwrap();
    assert!(deadline >= before + 2_400_000);
    // A missing/busy incarnation follows the busy deferral branch.
    assert!(
        kelpie
            .reminders_after_snapshot(collected, vec![], &[])
            .unwrap()
            .is_empty()
    );
    let after = kelpie
        .store()
        .reminder_info(ask.message_id)
        .unwrap()
        .unwrap()
        .next_eligible_at_ms
        .unwrap();
    assert_eq!(after, deadline);
    assert!(
        kelpie
            .store()
            .due_reminders(deadline - 1)
            .unwrap()
            .is_empty()
    );
    assert_eq!(kelpie.store().due_reminders(deadline).unwrap().len(), 1);
}
