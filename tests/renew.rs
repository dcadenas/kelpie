//! Renew phase machine, quarantine, and policy lifetime.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::thread;
use std::time::Duration;

use kelpie::domain::{
    InitialMessageIntent, InitialMessageKind, Parent, RenewIntent, RenewPhase, RenewTimeout,
    StartIntent,
};
use kelpie::herdr::{AgentObservation, HerdrClient};
use kelpie::slice::{Kelpie, SliceError};
use kelpie::store::{DeclaredStart, Store, store_clock_ms};

fn intent(name: &str, pane: &str, terminal: &str, key: &str, backend: &str) -> StartIntent {
    StartIntent {
        public_name: name.into(),
        logical_agent_id: None,
        parent: Parent::Parentless,
        herdr_session: "test".into(),
        pane_id: pane.into(),
        expected_terminal_id: terminal.into(),
        backend_kind: backend.into(),
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
    backend: &str,
) -> DeclaredStart {
    let declared = store
        .declare_start(&intent(name, pane, terminal, key, backend))
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
                agent: Some(backend.into()),
                interactive_ready: true,
                launch_pending: false,
                agent_session: None,
            },
            None,
        )
        .expect("ready");
    declared
}

fn renew_intent(worker: &DeclaredStart, every_ms: Option<i64>) -> RenewIntent {
    RenewIntent {
        logical_agent_id: worker.logical_agent_id,
        incarnation_id: worker.incarnation_id,
        requester_agent_id: worker.logical_agent_id,
        prepare_prompt: "save progress to progress.md".into(),
        resume_prompt: "read progress.md and continue".into(),
        on_timeout: RenewTimeout::Abort,
        prepare_timeout_ms: 60_000,
        every_ms,
        scheduled_at_ms: store_clock_ms().expect("clock"),
    }
}

/// A policy one agent arms on another, which is the shape a cancel has to undo.
fn renew_intent_from(
    worker: &DeclaredStart,
    requester: kelpie::domain::LogicalAgentId,
    every_ms: Option<i64>,
) -> RenewIntent {
    RenewIntent {
        requester_agent_id: requester,
        ..renew_intent(worker, every_ms)
    }
}

/// Reaching the clear requires an ask message, so make a real one.
fn armed_prepare(
    store: &mut Store,
    worker: &DeclaredStart,
    renew_id: kelpie::domain::RenewId,
) -> kelpie::domain::MessageId {
    let ask = store
        .create_ask_with_schedule(
            worker.logical_agent_id,
            worker.logical_agent_id,
            worker.incarnation_id,
            "save progress to progress.md",
            &format!("prepare:{renew_id}"),
            None,
            None,
            false,
        )
        .expect("prepare ask");
    store
        .mark_renew_preparing(
            renew_id,
            ask.message_id,
            store_clock_ms().expect("clock") + 60_000,
        )
        .expect("preparing");
    ask.message_id
}

/// Arm the prepare ask and answer it exactly as an agent does: a final reply
/// whose accepted delivery resolves the obligation.
///
/// That acceptance is a prompt into the agent's own pane whenever it renews
/// itself, which is what the clear then has to be spaced from.
fn settled_prepare(
    store: &mut Store,
    worker: &DeclaredStart,
    renew_id: kelpie::domain::RenewId,
) -> kelpie::domain::MessageId {
    let ask = store
        .create_ask_with_schedule(
            worker.logical_agent_id,
            worker.logical_agent_id,
            worker.incarnation_id,
            "save progress to progress.md",
            &format!("prepare:{renew_id}"),
            None,
            None,
            false,
        )
        .expect("prepare ask");
    store
        .mark_renew_preparing(
            renew_id,
            ask.message_id,
            store_clock_ms().expect("clock") + 60_000,
        )
        .expect("preparing");
    let reply = store
        .create_reply(
            ask.message_id,
            worker.logical_agent_id,
            "checkpoint written",
            kelpie::domain::ReplyDisposition::Final,
            &format!("reply:{renew_id}"),
        )
        .expect("final reply");
    let request = format!("reply-request:{renew_id}");
    let operation_id = reply.operation_id.expect("pane reply operation");
    store
        .begin_attempt(operation_id, worker.incarnation_id, &request)
        .expect("attempt");
    store
        .mark_submitted(operation_id, 1, &request)
        .expect("submitted");
    store
        .accept_delivery(operation_id, worker.incarnation_id, "w:p1", "term-1")
        .expect("accepted");
    ask.message_id
}

/// A clear deadline far enough out that these tests exercise the wait itself
/// rather than the stall report.
fn far_clear_deadline() -> i64 {
    store_clock_ms().expect("clock") + 3_600_000
}

fn earn_interval(store: &mut Store, renew_id: kelpie::domain::RenewId, every_ms: i64) {
    let mut t = store
        .scheduled_interval_renews()
        .expect("clocks")
        .into_iter()
        .find(|clock| clock.renew_id == renew_id)
        .and_then(|clock| clock.occupancy_sampled_at_ms)
        .unwrap_or_else(|| store_clock_ms().expect("clock"));
    let mut earned = 0;
    while earned < every_ms {
        let step = 1_000.min(every_ms - earned);
        t = t.saturating_add(step);
        store
            .accrue_renew_occupancy(renew_id, true, t)
            .expect("earn active occupancy");
        earned += step;
    }
}

/// Serve every request that arrives within a short window, then stop.
///
/// Bounded by time rather than by a request count: the point of these tests is
/// what Kelpie chooses to send, so a server that blocks waiting for an exact
/// number of connections turns "sent one request fewer" into a hang instead of
/// a failed assertion.
fn serve_briefly(
    listener: &UnixListener,
    mut respond: impl FnMut(&serde_json::Value) -> serde_json::Value,
) -> Vec<serde_json::Value> {
    listener.set_nonblocking(true).expect("nonblocking");
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut requests = Vec::new();
    while std::time::Instant::now() < deadline {
        let Ok((mut stream, _)) = listener.accept() else {
            thread::sleep(Duration::from_millis(5));
            continue;
        };
        stream.set_nonblocking(false).expect("blocking stream");
        let mut line = String::new();
        if BufReader::new(stream.try_clone().expect("clone"))
            .read_line(&mut line)
            .is_err()
            || line.trim().is_empty()
        {
            continue;
        }
        let request: serde_json::Value = serde_json::from_str(&line).expect("json");
        let result = respond(&request);
        requests.push(request.clone());
        serde_json::to_writer(
            &mut stream,
            &serde_json::json!({"id": request["id"], "result": result}),
        )
        .expect("write");
        stream.write_all(b"\n").expect("finish");
    }
    requests
}

fn agent_info(backend: &str, session: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "agent_info",
        "agent": {
            "terminal_id": "term-1",
            "pane_id": "w:p1",
            "name": "worker",
            "agent": backend,
            "interactive_ready": true,
            "launch_pending": false,
            "agent_session": session
        }
    })
}

#[test]
fn a_backend_without_a_verified_clear_command_is_refused_before_durable_intent() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "cursor");
    let socket = std::path::PathBuf::from("/nonexistent/herdr.sock");
    let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_millis(50)));

    let error = kelpie
        .renew(
            worker.logical_agent_id,
            worker.logical_agent_id,
            worker.incarnation_id,
            "save progress",
            "continue",
            RenewTimeout::Abort,
            60_000,
            None,
            store_clock_ms().expect("clock"),
        )
        .expect_err("cursor has no verified clear command");
    assert!(matches!(error, SliceError::UnsupportedBackend { .. }));

    // Refused before anything durable: a wrong clear command destroys a context
    // and re-seeds nothing, so this must not leave a renew behind to retry.
    assert!(
        kelpie
            .store_mut()
            .actionable_renews(store_clock_ms().expect("clock") + 60_000)
            .expect("actionable")
            .is_empty()
    );
}

#[test]
fn every_backend_with_a_verified_clear_command_can_be_renewed() {
    // Renew is a Kelpie operation, not a Claude one. A backend drops out of this
    // list only by losing its verified clear protocol, never by being unnamed.
    for backend_kind in ["claude", "codex", "grok", "pi", "opencode"] {
        let mut store = Store::in_memory().expect("store");
        let worker = ready(
            &mut store,
            "worker",
            "w:p1",
            "term-1",
            "start",
            backend_kind,
        );
        let socket = std::path::PathBuf::from("/nonexistent/herdr.sock");
        let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_millis(50)));

        kelpie
            .renew(
                worker.logical_agent_id,
                worker.logical_agent_id,
                worker.incarnation_id,
                "save progress",
                "continue",
                RenewTimeout::Abort,
                60_000,
                None,
                store_clock_ms().expect("clock"),
            )
            .unwrap_or_else(|error| panic!("{backend_kind} should renew: {error}"));
    }
}

#[test]
fn a_lazily_rotating_backend_injects_on_the_gate_and_proves_it_afterwards() {
    // opencode's `/clear` is a client-side route change: no session is created
    // until the next prompt, so waiting for rotation before injecting would
    // deadlock. The injection is what produces the rotation, and the rotation
    // still has to be seen before the renew is called done.
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("herdr.sock");
    let listener = UnixListener::bind(&socket).expect("bind");
    let server = thread::spawn(move || {
        // Before the injection the session is unchanged; after it, opencode has
        // allocated a new one.
        let mut injected = false;
        serve_briefly(&listener, move |request| {
            if request["method"] == "agent.prompt" {
                injected = true;
            }
            agent_info("opencode", if injected { "sess-2" } else { "sess-1" })
        })
    });

    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "opencode");
    let renew_id = store
        .create_renew(&renew_intent(&worker, None))
        .expect("create");
    armed_prepare(&mut store, &worker, renew_id);
    store.mark_renew_ready(renew_id).expect("ready");
    // Clearing, with the injection gate already open and the session still the
    // pre-clear one: exactly the state opencode sits in after its clear.
    store
        .mark_renew_clearing(
            renew_id,
            "\"sess-1\"",
            far_clear_deadline(),
            Some(store_clock_ms().expect("clock") - 1),
        )
        .expect("clearing");

    let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_secs(1)));
    // Injects despite the unchanged session, which an OnClear backend would
    // never do.
    assert_eq!(kelpie.drive_renews().expect("inject"), 1);
    // Then proves it: the new session appears and the renew completes.
    assert_eq!(kelpie.drive_renews().expect("confirm"), 1);

    let prompts: Vec<String> = server
        .join()
        .expect("server")
        .into_iter()
        .filter(|request| request["method"] == "agent.prompt")
        .map(|request| request["params"]["text"].as_str().expect("text").into())
        .collect();
    assert_eq!(prompts.len(), 1, "the resume prompt is sent exactly once");
    assert!(prompts[0].contains("You are a continuation."));
    assert!(
        kelpie
            .store_mut()
            .actionable_renews(store_clock_ms().expect("clock"))
            .expect("actionable")
            .into_iter()
            .all(|item| item.renew_id != renew_id),
        "a proven renew is done"
    );
}

#[test]
fn a_lazily_rotating_backend_that_does_not_rotate_after_injection_is_not_done() {
    // The failure the inverted barrier exists to catch: the clear never landed,
    // so the resume prompt went into the context it was meant to replace. The
    // renew must not record that as a success.
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("herdr.sock");
    let listener = UnixListener::bind(&socket).expect("bind");
    // The session never changes, however many prompts arrive.
    let server =
        thread::spawn(move || serve_briefly(&listener, |_| agent_info("opencode", "sess-1")));

    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "opencode");
    let renew_id = store
        .create_renew(&renew_intent(&worker, None))
        .expect("create");
    armed_prepare(&mut store, &worker, renew_id);
    store.mark_renew_ready(renew_id).expect("ready");
    let now = store_clock_ms().expect("clock");
    store
        .mark_renew_clearing(renew_id, "\"sess-1\"", now - 1_000, Some(now - 1))
        .expect("clearing");

    let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_secs(1)));
    kelpie.drive_renews().expect("inject");
    kelpie.drive_renews().expect("confirm attempt");
    server.join().expect("server");

    let item = kelpie
        .store_mut()
        .actionable_renews(store_clock_ms().expect("clock"))
        .expect("actionable")
        .into_iter()
        .find(|item| item.renew_id == renew_id)
        .expect("an unproven renew is still being driven");
    assert_eq!(
        item.phase,
        RenewPhase::Injected,
        "without a rotation the clear is unproven, so the renew is not done"
    );
    let stalls: Vec<_> = kelpie
        .store_mut()
        .operator_notices()
        .expect("notices")
        .into_iter()
        .filter(|notice| notice.body.contains("has not rotated"))
        .collect();
    assert_eq!(stalls.len(), 1, "and the operator is told exactly once");
}

#[test]
fn the_clear_is_not_submitted_back_to_back_with_the_reply_that_authorised_it() {
    // An agent that renews itself is its own waiter, so the final reply that
    // authorises the clear is delivered into the very pane about to be cleared.
    // Submitted milliseconds later, the clear is taken as part of that same
    // input and nothing is cleared — the failure this gap exists to prevent.
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("herdr.sock");
    let listener = UnixListener::bind(&socket).expect("bind");
    let server =
        thread::spawn(move || serve_briefly(&listener, |_| agent_info("opencode", "sess-1")));

    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "opencode");
    let renew_id = store
        .create_renew(&renew_intent(&worker, None))
        .expect("create");
    settled_prepare(&mut store, &worker, renew_id);

    let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_secs(1)));
    // The reply lands and the renew becomes Ready.
    assert_eq!(kelpie.drive_renews().expect("settle"), 1);
    // Then nothing happens, however often the driver runs: the clear waits out
    // the reply rather than following it into the same pane.
    assert_eq!(kelpie.drive_renews().expect("hold"), 0);
    assert_eq!(kelpie.drive_renews().expect("hold again"), 0);

    // Once the gap has elapsed the same renew clears normally.
    kelpie.set_prompt_settle_delay_ms(0);
    assert_eq!(kelpie.drive_renews().expect("clear"), 1);

    let clears: Vec<String> = server
        .join()
        .expect("server")
        .into_iter()
        .filter(|request| request["method"] == "agent.prompt")
        .map(|request| request["params"]["text"].as_str().expect("text").into())
        .collect();
    assert_eq!(clears, vec!["/clear".to_string()]);
}

#[test]
fn a_cycle_whose_clear_is_never_proven_ends_and_arms_the_next_one() {
    // The wedge this closes: injected, unrotated, and a policy that re-arms only
    // on completion waits forever on a rotation that is not coming. The context
    // was probably never bounded, so the standing rule that bounds it is the
    // last thing that should stop.
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("herdr.sock");
    let listener = UnixListener::bind(&socket).expect("bind");
    let server =
        thread::spawn(move || serve_briefly(&listener, |_| agent_info("opencode", "sess-1")));

    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "opencode");
    let every_ms = 45 * 60 * 1_000;
    let renew_id = store
        .create_renew(&renew_intent(&worker, Some(every_ms)))
        .expect("create");
    armed_prepare(&mut store, &worker, renew_id);
    store.mark_renew_ready(renew_id).expect("ready");
    // A clear deadline already long past, so the abandon bound is past too.
    let now = store_clock_ms().expect("clock");
    store
        .mark_renew_clearing(renew_id, "\"sess-1\"", now - 30 * 60 * 1_000, Some(now - 1))
        .expect("clearing");

    let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_secs(1)));
    kelpie.drive_renews().expect("inject");
    kelpie.drive_renews().expect("give up");
    server.join().expect("server");

    let successor_id = kelpie
        .store_mut()
        .scheduled_interval_renews()
        .expect("clocks")
        .into_iter()
        .find(|clock| clock.renew_id != renew_id)
        .expect("the policy arms its next cycle")
        .renew_id;
    earn_interval(kelpie.store_mut(), successor_id, every_ms);
    let actionable = kelpie
        .store_mut()
        .actionable_renews(store_clock_ms().expect("clock"))
        .expect("actionable");
    assert!(
        actionable.iter().all(|item| item.renew_id != renew_id),
        "the unprovable cycle is over rather than driven forever"
    );
    let successor = actionable
        .iter()
        .find(|item| item.renew_id == successor_id)
        .expect("the successor is due after active occupancy");
    assert_eq!(successor.phase, RenewPhase::Scheduled);
    assert_eq!(successor.cycle, 2);
    assert!(
        kelpie
            .store_mut()
            .operator_notices()
            .expect("notices")
            .iter()
            .any(|notice| notice.body.contains("never proven cleared")),
        "and the operator is told the cycle was abandoned"
    );
}

#[test]
fn only_a_resolved_prepare_obligation_opens_the_clear() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let renew_id = store
        .create_renew(&renew_intent(&worker, None))
        .expect("create");
    armed_prepare(&mut store, &worker, renew_id);

    let now = store_clock_ms().expect("clock");
    let item = store
        .actionable_renews(now)
        .expect("actionable")
        .into_iter()
        .find(|item| item.renew_id == renew_id)
        .expect("renew is actionable");
    assert_eq!(item.phase, RenewPhase::Preparing);
    // Unanswered: the agent has not said its checkpoint exists.
    assert_ne!(
        item.prepare_obligation_state,
        Some(kelpie::domain::ObligationState::Resolved)
    );
    // And the clear cannot be entered from here.
    store
        .mark_renew_clearing(renew_id, "\"sess-1\"", far_clear_deadline(), None)
        .expect_err("clearing requires the ready phase");
}

#[test]
fn a_prepare_timeout_never_clears_when_the_caller_said_abort() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let renew_id = store
        .create_renew(&renew_intent(&worker, None))
        .expect("create");
    armed_prepare(&mut store, &worker, renew_id);
    store.mark_renew_timed_out(renew_id).expect("timed out");
    assert_eq!(
        store
            .abort_renew(renew_id, "prepare deadline elapsed")
            .expect("abort"),
        None,
        "a one-shot renew arms no successor"
    );

    // Aborted is terminal, and the context was never touched.
    assert!(
        store
            .actionable_renews(store_clock_ms().expect("clock") + 60_000)
            .expect("actionable")
            .iter()
            .all(|item| item.renew_id != renew_id)
    );
    store
        .mark_renew_ready(renew_id)
        .expect_err("an aborted renew cannot be resurrected into a clear");
}

#[test]
fn an_aborted_cycle_takes_its_prepare_ask_with_it() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let renew_id = store
        .create_renew(&renew_intent(&worker, None))
        .expect("create");
    let ask = armed_prepare(&mut store, &worker, renew_id);
    store.mark_renew_timed_out(renew_id).expect("timed out");
    store
        .abort_renew(renew_id, "prepare deadline elapsed")
        .expect("abort");

    // The cycle that asked is gone, so the question goes with it. Left open it
    // would be a durable reply obligation, and reminders, about a checkpoint
    // for a clear that will never happen.
    assert_eq!(
        store.obligation_state(ask).expect("state"),
        kelpie::domain::ObligationState::Cancelled
    );
    assert!(
        store
            .pending_obligations(worker.logical_agent_id)
            .expect("pending")
            .is_empty(),
        "and the agent is no longer told it owes an answer"
    );
}

#[test]
fn proceeding_without_a_reply_settles_the_ask_it_gave_up_on() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let renew_id = store
        .create_renew(&renew_intent(&worker, None))
        .expect("create");
    let ask = armed_prepare(&mut store, &worker, renew_id);
    store.mark_renew_timed_out(renew_id).expect("timed out");

    // `proceed` clears without the confirmation it asked for. The ask is
    // settled at that decision rather than at the end of the cycle: the moment
    // the answer stopped mattering is the moment it stopped being owed.
    store.mark_renew_ready(renew_id).expect("proceed");
    assert_eq!(
        store.obligation_state(ask).expect("state"),
        kelpie::domain::ObligationState::Cancelled
    );
}

#[test]
fn a_reply_that_beat_the_deadline_is_never_overwritten_by_a_cancel() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let renew_id = store
        .create_renew(&renew_intent(&worker, None))
        .expect("create");
    let ask = settled_prepare(&mut store, &worker, renew_id);

    // Reaching Ready with the obligation already resolved is the ordinary path.
    // Cancelling only ever claims an unanswered ask, so the reply the agent did
    // send keeps its outcome.
    store.mark_renew_ready(renew_id).expect("ready");
    assert_eq!(
        store.obligation_state(ask).expect("state"),
        kelpie::domain::ObligationState::Resolved
    );
}

#[test]
fn a_skipped_cycle_does_not_disarm_the_policy_that_asked_for_it() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let every_ms = 45 * 60 * 1_000;
    let policy = store
        .create_renew(&renew_intent(&worker, Some(every_ms)))
        .expect("create policy");
    armed_prepare(&mut store, &worker, policy);
    store.mark_renew_timed_out(policy).expect("timed out");

    // The agent never confirmed, so this cycle is abandoned with its context
    // intact. The standing rule that bounds that context is not abandoned with
    // it: an agent too busy to checkpoint is the one that most needs the next
    // cycle to come around.
    let next = store
        .abort_renew(policy, "prepare deadline elapsed")
        .expect("abort")
        .expect("a policy arms its successor even when a cycle is skipped");
    earn_interval(&mut store, next, every_ms);

    let armed = store
        .actionable_renews(store_clock_ms().expect("clock"))
        .expect("actionable")
        .into_iter()
        .find(|item| item.renew_id == next)
        .expect("the successor cycle is scheduled");
    assert_eq!(armed.phase, RenewPhase::Scheduled);
    assert_eq!(armed.cycle, 2);
    assert_eq!(armed.every_ms, Some(every_ms));
}

#[test]
fn a_message_sent_during_a_renew_waits_instead_of_vanishing_with_the_context() {
    let mut store = Store::in_memory().expect("store");
    let sender = ready(&mut store, "sender", "w:p0", "term-0", "sender", "claude");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let renew_id = store
        .create_renew(&renew_intent(&worker, None))
        .expect("create");
    armed_prepare(&mut store, &worker, renew_id);

    let now = store_clock_ms().expect("clock");
    let tell = store
        .create_tell_with_due(
            sender.logical_agent_id,
            worker.logical_agent_id,
            worker.incarnation_id,
            "important",
            "queued-tell",
            Some(now),
        )
        .expect("queued tell");

    // Due while the renew is only preparing: the context still exists.
    assert!(
        store
            .due_deliveries(now)
            .expect("due")
            .iter()
            .any(|item| item.message_id == tell.message_id)
    );

    store.mark_renew_ready(renew_id).expect("ready");
    // From here the context is about to be discarded. Delivering now would
    // record `accepted` for a message the agent will never see.
    assert!(
        store
            .due_deliveries(now)
            .expect("due while ready")
            .iter()
            .all(|item| item.message_id != tell.message_id)
    );

    store
        .mark_renew_clearing(renew_id, "\"sess-1\"", far_clear_deadline(), None)
        .expect("clearing");
    assert!(
        store
            .due_deliveries(now)
            .expect("due while clearing")
            .iter()
            .all(|item| item.message_id != tell.message_id)
    );

    store.mark_renew_injected(renew_id).expect("injected");
    // Resumed: the message is owed again, unchanged and still queued.
    assert!(
        store
            .due_deliveries(now)
            .expect("due after inject")
            .iter()
            .any(|item| item.message_id == tell.message_id)
    );
}

#[test]
fn a_policy_rearms_with_the_next_cycle_and_a_one_shot_does_not() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");

    let once = store
        .create_renew(&renew_intent(&worker, None))
        .expect("create one-shot");
    armed_prepare(&mut store, &worker, once);
    store.mark_renew_ready(once).expect("ready");
    store
        .mark_renew_clearing(once, "\"sess-1\"", far_clear_deadline(), None)
        .expect("clearing");
    store.mark_renew_injected(once).expect("injected");
    assert_eq!(store.complete_renew(once).expect("complete"), None);

    let policy = store
        .create_renew(&renew_intent(&worker, Some(45 * 60 * 1_000)))
        .expect("create policy");
    armed_prepare(&mut store, &worker, policy);
    store.mark_renew_ready(policy).expect("ready");
    store
        .mark_renew_clearing(policy, "\"sess-2\"", far_clear_deadline(), None)
        .expect("clearing");
    store.mark_renew_injected(policy).expect("injected");
    let next = store
        .complete_renew(policy)
        .expect("complete")
        .expect("a policy arms its successor");
    earn_interval(&mut store, next, 45 * 60 * 1_000);

    let armed = store
        .actionable_renews(store_clock_ms().expect("clock"))
        .expect("actionable")
        .into_iter()
        .find(|item| item.renew_id == next)
        .expect("successor is scheduled");
    assert_eq!(armed.phase, RenewPhase::Scheduled);
    assert_eq!(
        armed.cycle, 2,
        "the resumed agent is told which cycle it is"
    );
    assert_eq!(armed.every_ms, Some(45 * 60 * 1_000));
}

#[test]
fn one_incarnation_cannot_be_cleared_by_two_rules_at_once() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    store
        .create_renew(&renew_intent(&worker, None))
        .expect("first");
    store
        .create_renew(&renew_intent(&worker, None))
        .expect_err("a second renew would clear a context the first is preparing");
}

#[test]
fn a_policy_ends_when_its_incarnation_stops_being_ready() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let policy = store
        .create_renew(&renew_intent(&worker, Some(1_000)))
        .expect("create policy");
    assert!(store.terminable_renews().expect("terminable").is_empty());

    store
        .request_retirement(worker.incarnation_id, "retire-worker")
        .expect("retiring");

    assert_eq!(
        store.terminable_renews().expect("terminable"),
        vec![policy],
        "a rule bound to an agent that no longer exists has nothing left to do"
    );
    store
        .terminate_renew(policy, "incarnation is no longer Ready")
        .expect("terminate");
    assert!(
        store
            .actionable_renews(store_clock_ms().expect("clock") + 60_000)
            .expect("actionable")
            .is_empty()
    );
}

/// A policy that ends must say so, and name enough to act on.
///
/// Driven through retirement because `terminable_renews` keys on "not Ready"
/// rather than on any particular terminal state; a binding lost to
/// reconciliation reaches this same path. The notice is the only signal that a
/// context stopped being bounded, since adoption restores addressing and not
/// the rule.
#[test]
fn a_terminated_policy_raises_an_operator_notice() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("herdr.sock");
    let _listener = UnixListener::bind(&socket).expect("bind");
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let every_ms = 45 * 60 * 1_000;
    let policy = store
        .create_renew(&renew_intent(&worker, Some(every_ms)))
        .expect("create policy");
    store
        .request_retirement(worker.incarnation_id, "retire-worker")
        .expect("retiring");

    let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_secs(1)));
    kelpie.drive_renews().expect("drive");

    let notices = kelpie.store_mut().operator_notices().expect("notices");
    let notice = notices
        .iter()
        .find(|notice| notice.body.contains(&policy.to_string()))
        .expect("the terminated policy is named");
    assert!(notice.body.contains("worker"), "{}", notice.body);
    assert!(
        notice.body.contains("no longer Ready"),
        "the reason is stated: {}",
        notice.body
    );
    assert!(
        notice.body.contains("re-arm"),
        "and what to do about it: {}",
        notice.body
    );
}

/// `report` answers whether a policy is armed, which nothing else did.
#[test]
fn report_carries_the_armed_renew_of_an_incarnation() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let every_ms = 45 * 60 * 1_000;

    let unarmed = store.report().expect("report");
    let incarnation = unarmed
        .agents
        .iter()
        .flat_map(|agent| agent.incarnations.iter())
        .find(|incarnation| incarnation.id == worker.incarnation_id)
        .expect("incarnation");
    assert!(
        incarnation.renew.is_none(),
        "an unarmed root reports no policy"
    );

    let policy = store
        .create_renew(&renew_intent(&worker, Some(every_ms)))
        .expect("create policy");
    let armed = store.report().expect("report");
    let renew = armed
        .agents
        .iter()
        .flat_map(|agent| agent.incarnations.iter())
        .find(|incarnation| incarnation.id == worker.incarnation_id)
        .and_then(|incarnation| incarnation.renew.as_ref())
        .expect("the armed policy is reported");
    assert_eq!(renew.id, policy);
    assert_eq!(renew.every_ms, Some(every_ms));
    assert_eq!(renew.cycle, 1);
    assert_eq!(renew.phase, RenewPhase::Scheduled);

    // A terminal policy is not an armed one.
    store
        .terminate_renew(policy, "incarnation is no longer Ready")
        .expect("terminate");
    let ended = store.report().expect("report");
    assert!(
        ended
            .agents
            .iter()
            .flat_map(|agent| agent.incarnations.iter())
            .find(|incarnation| incarnation.id == worker.incarnation_id)
            .and_then(|incarnation| incarnation.renew.as_ref())
            .is_none(),
        "a terminated policy stops being reported as armed"
    );
}

#[test]
fn a_terminated_policy_leaves_no_prepare_ask_behind() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let policy = store
        .create_renew(&renew_intent(&worker, Some(1_000)))
        .expect("create policy");
    let ask = armed_prepare(&mut store, &worker, policy);

    store
        .request_retirement(worker.incarnation_id, "retire-worker")
        .expect("retiring");
    store
        .terminate_renew(policy, "incarnation is no longer Ready")
        .expect("terminate");

    // The incarnation that was asked is gone. An obligation outlives its
    // runtime by design, so this one would have outlived the whole policy.
    assert_eq!(
        store.obligation_state(ask).expect("state"),
        kelpie::domain::ObligationState::Cancelled
    );
}

#[test]
fn a_rotated_session_reference_releases_the_resume_prompt() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("herdr.sock");
    let listener = UnixListener::bind(&socket).expect("bind");
    let server = thread::spawn(move || {
        let mut prompt = None;
        for expected in ["agent.get", "agent.prompt"] {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut line = String::new();
            BufReader::new(stream.try_clone().expect("clone"))
                .read_line(&mut line)
                .expect("read");
            let request: serde_json::Value = serde_json::from_str(&line).expect("json");
            assert_eq!(request["method"], expected);
            if expected == "agent.prompt" {
                prompt = Some(
                    request["params"]["text"]
                        .as_str()
                        .expect("text")
                        .to_string(),
                );
            }
            serde_json::to_writer(
                &mut stream,
                &serde_json::json!({
                    "id": request["id"],
                    "result": {
                        "type": "agent_info",
                        "agent": {
                            "terminal_id": "term-1",
                            "pane_id": "w:p1",
                            "name": "worker",
                            "agent": "claude",
                            "interactive_ready": true,
                            "launch_pending": false,
                            "agent_session": "sess-2"
                        }
                    }
                }),
            )
            .expect("write");
            stream.write_all(b"\n").expect("finish");
        }
        prompt.expect("resume prompt")
    });

    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let renew_id = store
        .create_renew(&renew_intent(&worker, None))
        .expect("create");
    armed_prepare(&mut store, &worker, renew_id);
    store.mark_renew_ready(renew_id).expect("ready");
    store
        .mark_renew_clearing(renew_id, "\"sess-1\"", far_clear_deadline(), None)
        .expect("clearing");

    let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_secs(1)));
    assert_eq!(kelpie.drive_renews().expect("drive"), 1);
    let prompt = server.join().expect("server");
    assert!(prompt.contains("You are a continuation."));
    assert!(prompt.contains("read progress.md and continue"));
}

#[test]
fn a_clear_that_never_rotates_is_reported_once_and_keeps_being_retried() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("herdr.sock");
    let listener = UnixListener::bind(&socket).expect("bind");
    // Two rotation probes, one per driver pass, both answering with the
    // pre-clear reference: the backend took the clear command and never
    // rotated, which is what an unverified clear command looks like from here.
    let server = thread::spawn(move || {
        let mut methods = Vec::new();
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut line = String::new();
            BufReader::new(stream.try_clone().expect("clone"))
                .read_line(&mut line)
                .expect("read");
            let request: serde_json::Value = serde_json::from_str(&line).expect("json");
            methods.push(request["method"].as_str().expect("method").to_string());
            serde_json::to_writer(
                &mut stream,
                &serde_json::json!({
                    "id": request["id"],
                    "result": {
                        "type": "agent_info",
                        "agent": {
                            "terminal_id": "term-1",
                            "pane_id": "w:p1",
                            "name": "worker",
                            "agent": "claude",
                            "interactive_ready": true,
                            "launch_pending": false,
                            "agent_session": "sess-1"
                        }
                    }
                }),
            )
            .expect("write");
            stream.write_all(b"\n").expect("finish");
        }
        methods
    });

    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let renew_id = store
        .create_renew(&renew_intent(&worker, None))
        .expect("create");
    armed_prepare(&mut store, &worker, renew_id);
    store.mark_renew_ready(renew_id).expect("ready");
    store
        .mark_renew_clearing(
            renew_id,
            "\"sess-1\"",
            store_clock_ms().expect("clock") - 1_000,
            None,
        )
        .expect("clearing");

    let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_secs(1)));
    kelpie.drive_renews().expect("first pass");
    kelpie.drive_renews().expect("second pass");

    let stalls: Vec<_> = kelpie
        .store_mut()
        .operator_notices()
        .expect("notices")
        .into_iter()
        .filter(|notice| notice.body.contains("has not rotated"))
        .collect();
    assert_eq!(
        stalls.len(),
        1,
        "a stall repeats every pass, so reporting it every pass would train an \
         operator to ignore the channel"
    );

    // The renew is still clearing: a deadline bounds the report, never the
    // injection. Abandoning here would leave the agent wiped and instructionless.
    let still_clearing = kelpie
        .store_mut()
        .actionable_renews(store_clock_ms().expect("clock"))
        .expect("actionable")
        .into_iter()
        .find(|item| item.renew_id == renew_id)
        .expect("renew is still being driven");
    assert_eq!(still_clearing.phase, RenewPhase::Clearing);

    // And nothing was submitted into the pane while the clear stayed unproven.
    let methods = server.join().expect("server");
    assert!(
        methods.iter().all(|method| method == "agent.get"),
        "only rotation probes may cross the wire: {methods:?}"
    );
}

#[test]
fn the_resume_prompt_waits_for_the_session_reference_to_actually_change() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("herdr.sock");
    let listener = UnixListener::bind(&socket).expect("bind");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut line = String::new();
        BufReader::new(stream.try_clone().expect("clone"))
            .read_line(&mut line)
            .expect("read");
        let request: serde_json::Value = serde_json::from_str(&line).expect("json");
        // Only the rotation probe may happen. An inject here would be the bug.
        assert_eq!(request["method"], "agent.get");
        serde_json::to_writer(
            &mut stream,
            &serde_json::json!({
                "id": request["id"],
                "result": {
                    "type": "agent_info",
                    "agent": {
                        "terminal_id": "term-1",
                        "pane_id": "w:p1",
                        "name": "worker",
                        "agent": "claude",
                        "interactive_ready": true,
                        "launch_pending": false,
                        "agent_session": "sess-1"
                    }
                }
            }),
        )
        .expect("write");
        stream.write_all(b"\n").expect("finish");
        // Any second request would mean the resume prompt was submitted into a
        // context that was never cleared.
        listener
            .set_nonblocking(true)
            .expect("stop blocking the join");
        listener.accept().is_ok()
    });

    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let renew_id = store
        .create_renew(&renew_intent(&worker, None))
        .expect("create");
    armed_prepare(&mut store, &worker, renew_id);
    store.mark_renew_ready(renew_id).expect("ready");
    store
        .mark_renew_clearing(renew_id, "\"sess-1\"", far_clear_deadline(), None)
        .expect("clearing");

    let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_secs(1)));
    assert_eq!(
        kelpie.drive_renews().expect("drive"),
        0,
        "an unchanged session reference means the clear has not landed yet"
    );
    let injected = server.join().expect("server");
    assert!(
        !injected,
        "no resume prompt may be sent before the rotation"
    );

    let still_clearing = kelpie
        .store_mut()
        .actionable_renews(store_clock_ms().expect("clock"))
        .expect("actionable")
        .into_iter()
        .find(|item| item.renew_id == renew_id)
        .expect("renew is still owed");
    assert_eq!(still_clearing.phase, RenewPhase::Clearing);
}

/// The `report` RPC must actually carry renew state over the socket.
///
/// A store field and a renderer that agree by hand-copied literals are not the
/// feature: what a caller reads is the JSON the daemon writes. This drives the
/// real `report` method through a bound daemon so a rename on either side is a
/// failing test rather than a silently empty field.
#[test]
fn the_report_rpc_carries_renew_state_over_the_socket() {
    use std::os::unix::net::UnixStream;
    use std::sync::mpsc;

    let directory = tempfile::tempdir().expect("tempdir");
    let herdr_socket = directory.path().join("herdr.sock");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let _listener = UnixListener::bind(&herdr_socket).expect("bind herdr");

    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let every_ms = 45 * 60 * 1_000;
    // Due in the future, so the daemon's own drive pass leaves it `scheduled`
    // and the assertion is about what `report` says, not about a race with it.
    let mut intent = renew_intent(&worker, Some(every_ms));
    intent.scheduled_at_ms = store_clock_ms().expect("clock") + every_ms;
    let policy = store.create_renew(&intent).expect("create policy");

    let kelpie = Kelpie::new(
        store,
        HerdrClient::new(&herdr_socket, Duration::from_secs(1)),
    );
    let mut daemon = kelpie::daemon::Daemon::bind(&kelpie_socket, kelpie).expect("daemon");

    let (tx, rx) = mpsc::channel();
    let (connected_tx, connected_rx) = mpsc::channel();
    let client_socket = kelpie_socket.clone();
    let client = thread::spawn(move || {
        let mut stream = UnixStream::connect(&client_socket).expect("connect");
        serde_json::to_writer(
            &mut stream,
            &serde_json::json!({"id":"r","method":"report","params":{}}),
        )
        .expect("request");
        stream.write_all(b"\n").expect("finish");
        // Signal after the whole line is written: one poll must find it.
        connected_tx.send(()).expect("signal");
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line).expect("read");
        tx.send(serde_json::from_str::<serde_json::Value>(&line).expect("json"))
            .expect("send");
    });
    connected_rx.recv().expect("connected");
    daemon.poll().expect("serve report");
    let response = rx.recv().expect("response");
    client.join().expect("client");

    let renew = response["result"]["agents"]
        .as_array()
        .expect("agents")
        .iter()
        .flat_map(|agent| agent["incarnations"].as_array().expect("incarnations"))
        .find_map(|incarnation| {
            let renew = &incarnation["renew"];
            (!renew.is_null()).then_some(renew)
        })
        .expect("the armed policy crosses the socket");

    assert_eq!(renew["renew_id"], policy.to_string());
    assert_eq!(renew["phase"], "scheduled");
    assert_eq!(renew["cycle"], 1);
    assert_eq!(renew["every_ms"], every_ms);
    assert!(
        renew["cycle_due_at_ms"].is_i64(),
        "the due time is a number: {renew}"
    );
}

/// The incident this operation exists for: a policy armed on the wrong agent.
///
/// The target never asked for it, so the target has to be able to end it. The
/// prepare ask goes with it, because an obligation outlives its runtime and
/// nothing else would ever settle a question the cancelled cycle asked.
#[test]
fn the_target_of_a_policy_it_never_asked_for_can_end_it() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("herdr.sock");
    let _listener = UnixListener::bind(&socket).expect("bind");
    let mut store = Store::in_memory().expect("store");
    let stranger = ready(
        &mut store,
        "coord",
        "w:p9",
        "term-9",
        "start-coord",
        "claude",
    );
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let policy = store
        .create_renew(&renew_intent_from(
            &worker,
            stranger.logical_agent_id,
            Some(45 * 60 * 1_000),
        ))
        .expect("create policy");
    let ask = armed_prepare(&mut store, &worker, policy);

    let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_secs(1)));
    kelpie
        .cancel_renew(policy, worker.logical_agent_id, "armed on the wrong agent")
        .expect("the target may end a policy aimed at it");

    assert!(
        kelpie
            .store_mut()
            .actionable_renews(store_clock_ms().expect("clock") + 60_000)
            .expect("actionable")
            .is_empty(),
        "a cancelled policy never clears anything again"
    );
    let pending = kelpie
        .store_mut()
        .pending_obligations(worker.logical_agent_id)
        .expect("pending");
    assert!(
        !pending.iter().any(|item| item.ask_message_id == ask),
        "the cancelled cycle takes its unanswered question with it"
    );
}

/// A cancel any agent could call is a way to disarm somebody else's supervision.
#[test]
fn an_unrelated_agent_cannot_disarm_a_policy() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("herdr.sock");
    let _listener = UnixListener::bind(&socket).expect("bind");
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let bystander = ready(
        &mut store,
        "other",
        "w:p8",
        "term-8",
        "start-other",
        "claude",
    );
    let policy = store
        .create_renew(&renew_intent(&worker, Some(45 * 60 * 1_000)))
        .expect("create policy");

    let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_secs(1)));
    let refusal = kelpie
        .cancel_renew(policy, bystander.logical_agent_id, "not mine to end")
        .expect_err("neither the requester nor the target");
    let message = refusal.to_string();
    assert!(
        message.contains("requester") && message.contains("target"),
        "the refusal says who may cancel: {message}"
    );
    assert_eq!(
        kelpie
            .store_mut()
            .terminable_renews()
            .expect("terminable")
            .len(),
        0,
        "and the policy is untouched"
    );
    assert!(
        !kelpie
            .store_mut()
            .actionable_renews(store_clock_ms().expect("clock") + 60_000)
            .expect("actionable")
            .is_empty(),
        "the policy is still armed after a refused cancel"
    );
}

/// Cancelling between the clear and the injection would strand the agent.
///
/// The context is already gone at that point and only the resume prompt brings
/// it back, which must never be abandoned. The refusal has to say the wait is
/// temporary.
#[test]
fn a_cancel_will_not_abandon_a_cycle_that_has_already_cleared() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("herdr.sock");
    let _listener = UnixListener::bind(&socket).expect("bind");
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let policy = store
        .create_renew(&renew_intent(&worker, Some(45 * 60 * 1_000)))
        .expect("create policy");
    armed_prepare(&mut store, &worker, policy);
    store.mark_renew_ready(policy).expect("ready");
    store
        .mark_renew_clearing(policy, "\"sess-1\"", far_clear_deadline(), None)
        .expect("clearing");

    let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_secs(1)));
    let refusal = kelpie
        .cancel_renew(policy, worker.logical_agent_id, "changed my mind")
        .expect_err("the resume prompt is still owed");
    let message = refusal.to_string();
    assert!(
        message.contains("emptied context"),
        "the refusal says what cancelling now would cost: {message}"
    );
    assert!(
        message.contains("once the cycle finishes"),
        "and that the caller may cancel later: {message}"
    );
}

/// A deliberate cancel is still a context that stopped being bounded.
#[test]
fn a_cancelled_policy_says_who_ended_it_and_why() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("herdr.sock");
    let _listener = UnixListener::bind(&socket).expect("bind");
    let mut store = Store::in_memory().expect("store");
    let stranger = ready(
        &mut store,
        "coord",
        "w:p9",
        "term-9",
        "start-coord",
        "claude",
    );
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let policy = store
        .create_renew(&renew_intent_from(
            &worker,
            stranger.logical_agent_id,
            Some(45 * 60 * 1_000),
        ))
        .expect("create policy");

    let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_secs(1)));
    kelpie
        .cancel_renew(
            policy,
            stranger.logical_agent_id,
            "aimed at the wrong agent",
        )
        .expect("cancel");

    let notices = kelpie.store_mut().operator_notices().expect("notices");
    let notice = notices
        .iter()
        .find(|notice| notice.body.contains(&policy.to_string()))
        .expect("the cancelled policy is named");
    assert!(
        notice.body.contains("worker"),
        "the target is named: {}",
        notice.body
    );
    assert!(
        notice.body.contains("coord"),
        "and so is whoever ended it: {}",
        notice.body
    );
    assert!(
        notice.body.contains("aimed at the wrong agent"),
        "and the stated reason: {}",
        notice.body
    );
    assert!(
        notice.body.contains("45m"),
        "the interval is readable: {}",
        notice.body
    );
}

/// Send one request to a bound daemon and return its response.
///
/// One request per connection, which is what the daemon serves, so a test that
/// needs to observe an effect asks for it in a second exchange rather than
/// reaching past the socket into the store.
fn daemon_rpc(
    daemon: &mut kelpie::daemon::Daemon,
    socket: &std::path::Path,
    request: &serde_json::Value,
) -> serde_json::Value {
    use std::os::unix::net::UnixStream;
    use std::sync::mpsc;

    let (tx, rx) = mpsc::channel();
    let (connected_tx, connected_rx) = mpsc::channel();
    let client_socket = socket.to_path_buf();
    let request = request.clone();
    let client = thread::spawn(move || {
        let mut stream = UnixStream::connect(&client_socket).expect("connect");
        serde_json::to_writer(&mut stream, &request).expect("request");
        stream.write_all(b"\n").expect("finish");
        // Signal after the whole line is written: one poll must find it.
        connected_tx.send(()).expect("signal");
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line).expect("read");
        tx.send(serde_json::from_str::<serde_json::Value>(&line).expect("json"))
            .expect("send");
    });
    connected_rx.recv().expect("connected");
    daemon.poll().expect("serve");
    let response = rx.recv().expect("response");
    client.join().expect("client");
    response
}

/// The cancel has to be reachable over the socket, not only in the store.
///
/// A store method no request handler reaches is what `terminate_renew` already
/// was, and being unreachable is the reason a policy armed on the wrong agent
/// could not be undone at all. The effect is read back through `report`, so
/// this proves the whole path a caller actually has.
#[test]
fn the_cancel_rpc_ends_a_policy_over_the_socket() {
    let directory = tempfile::tempdir().expect("tempdir");
    let herdr_socket = directory.path().join("herdr.sock");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let _listener = UnixListener::bind(&herdr_socket).expect("bind herdr");

    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let every_ms = 45 * 60 * 1_000;
    let mut intent = renew_intent(&worker, Some(every_ms));
    intent.scheduled_at_ms = store_clock_ms().expect("clock") + every_ms;
    let policy = store.create_renew(&intent).expect("create policy");

    let kelpie = Kelpie::new(
        store,
        HerdrClient::new(&herdr_socket, Duration::from_secs(1)),
    );
    let mut daemon = kelpie::daemon::Daemon::bind(&kelpie_socket, kelpie).expect("daemon");

    let armed = daemon_rpc(
        &mut daemon,
        &kelpie_socket,
        &serde_json::json!({"id":"r1","method":"report","params":{}}),
    );
    assert!(
        armed_renew(&armed).is_some(),
        "the policy starts out armed: {armed}"
    );

    let cancelled = daemon_rpc(
        &mut daemon,
        &kelpie_socket,
        &serde_json::json!({
            "id": "c",
            "method": "renew.cancel",
            "params": {
                "renew_id": policy.to_string(),
                "requester_agent_id": worker.logical_agent_id.to_string(),
                "reason": "armed on the wrong agent"
            }
        }),
    );
    assert_eq!(cancelled["result"]["renew_id"], policy.to_string());
    assert!(
        cancelled["result"]["notice_id"].is_string(),
        "the cancel is announced: {cancelled}"
    );

    let after = daemon_rpc(
        &mut daemon,
        &kelpie_socket,
        &serde_json::json!({"id":"r2","method":"report","params":{}}),
    );
    assert!(
        armed_renew(&after).is_none(),
        "and the policy is no longer armed: {after}"
    );
}

/// The first armed renew a `report` response mentions, if any.
fn armed_renew(response: &serde_json::Value) -> Option<serde_json::Value> {
    response["result"]["agents"]
        .as_array()
        .expect("agents")
        .iter()
        .flat_map(|agent| agent["incarnations"].as_array().expect("incarnations"))
        .find_map(|incarnation| {
            let renew = &incarnation["renew"];
            (!renew.is_null()).then(|| renew.clone())
        })
}

/// The wire must refuse an alias too, or the CLI refusal is only advice.
///
/// Anything can open this socket and send JSON. If the daemon still resolved a
/// live name, the protection would be a property of one client rather than of
/// the operation.
#[test]
fn the_renew_rpc_will_not_resolve_an_alias() {
    let directory = tempfile::tempdir().expect("tempdir");
    let herdr_socket = directory.path().join("herdr.sock");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let _listener = UnixListener::bind(&herdr_socket).expect("bind herdr");

    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");

    let kelpie = Kelpie::new(
        store,
        HerdrClient::new(&herdr_socket, Duration::from_secs(1)),
    );
    let mut daemon = kelpie::daemon::Daemon::bind(&kelpie_socket, kelpie).expect("daemon");

    let response = daemon_rpc(
        &mut daemon,
        &kelpie_socket,
        &serde_json::json!({
            "id": "r",
            "method": "renew",
            "params": {
                "requester": worker.logical_agent_id.to_string(),
                "recipient_alias": "worker",
                "prepare_prompt": "checkpoint",
                "prompt": "resume",
                "on_timeout": "abort",
                "prepare_timeout_ms": 60_000
            }
        }),
    );
    let message = response["error"]["message"]
        .as_str()
        .expect("an alias is refused");
    assert!(
        message.contains("does not resolve an alias"),
        "and says why: {message}"
    );

    let after = daemon_rpc(
        &mut daemon,
        &kelpie_socket,
        &serde_json::json!({"id":"r2","method":"report","params":{}}),
    );
    assert!(
        armed_renew(&after).is_none(),
        "and nothing was armed: {after}"
    );
}

#[test]
fn the_renew_rpc_refuses_every_combined_with_a_due_time() {
    let directory = tempfile::tempdir().expect("tempdir");
    let herdr_socket = directory.path().join("herdr.sock");
    let kelpie_socket = directory.path().join("kelpie.sock");
    let _listener = UnixListener::bind(&herdr_socket).expect("bind herdr");

    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");

    let kelpie = Kelpie::new(
        store,
        HerdrClient::new(&herdr_socket, Duration::from_secs(1)),
    );
    let mut daemon = kelpie::daemon::Daemon::bind(&kelpie_socket, kelpie).expect("daemon");

    let response = daemon_rpc(
        &mut daemon,
        &kelpie_socket,
        &serde_json::json!({
            "id": "r",
            "method": "renew",
            "params": {
                "requester": worker.logical_agent_id.to_string(),
                "recipient": worker.logical_agent_id.to_string(),
                "recipient_incarnation": worker.incarnation_id.to_string(),
                "prepare_prompt": "checkpoint",
                "prompt": "resume",
                "on_timeout": "abort",
                "prepare_timeout_ms": 60_000,
                "every_ms": 2_700_000,
                "due_at_ms": store_clock_ms().expect("clock") + 3_600_000
            }
        }),
    );
    let message = response["error"]["message"]
        .as_str()
        .expect("the combination is refused");
    assert!(message.contains("not both"), "and says why: {message}");

    let after = daemon_rpc(
        &mut daemon,
        &kelpie_socket,
        &serde_json::json!({"id":"r2","method":"report","params":{}}),
    );
    assert!(
        armed_renew(&after).is_none(),
        "and nothing was armed: {after}"
    );
}

/// A renew cycle must not depend on any agent other than the one being cleared.
///
/// The prepare obligation is what authorises the clear, and an obligation
/// resolves only on accepted delivery to its waiting agent. Owing it to whoever
/// armed the policy made a destructive local operation depend on a third party
/// being Ready — and because `arm_next_renew_cycle` copies `requester_agent_id`
/// forward, a policy armed by an agent that later retires could never complete
/// any cycle. It looked armed and healthy in `report` while never once running.
#[test]
fn a_cycle_completes_when_the_agent_that_armed_it_is_gone() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("herdr.sock");
    let listener = UnixListener::bind(&socket).expect("bind");
    let server =
        thread::spawn(move || serve_briefly(&listener, |_request| agent_info("claude", "sess-1")));

    let mut store = Store::in_memory().expect("store");
    let coordinator = ready(&mut store, "coord", "w:p1", "term-1", "start", "claude");
    let leaf = ready(&mut store, "leaf", "w:p2", "term-2", "start-leaf", "claude");
    let renew_id = store
        .create_renew(&renew_intent_from(
            &coordinator,
            leaf.logical_agent_id,
            Some(45 * 60 * 1_000),
        ))
        .expect("create policy");
    // The leaf retires before the cycle fires, exactly as the review leaf did.
    store
        .request_retirement(leaf.incarnation_id, "retire-leaf")
        .expect("retiring");

    let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_secs(1)));
    assert_eq!(kelpie.drive_renews().expect("prepare"), 1);

    // The question is owed by the agent being renewed, so it is the coordinator
    // that can answer it — not the leaf that is no longer Ready.
    let owed = kelpie
        .store_mut()
        .pending_obligations(coordinator.logical_agent_id)
        .expect("pending");
    assert_eq!(owed.len(), 1, "the coordinator owes its own prepare");
    assert!(
        kelpie
            .store_mut()
            .pending_obligations(leaf.logical_agent_id)
            .expect("pending")
            .is_empty(),
        "and the retiring leaf owes nothing"
    );

    let ask = owed[0].ask_message_id;
    let reply = kelpie
        .reply(
            ask,
            coordinator.logical_agent_id,
            "checkpoint written",
            kelpie::domain::ReplyDisposition::Final,
            "prepare-reply",
        )
        .expect("a final reply is deliverable with the requester gone");
    assert_eq!(reply.disposition, kelpie::domain::ReplyDisposition::Final);

    assert_eq!(kelpie.drive_renews().expect("settle"), 1);
    let settled = kelpie
        .store_mut()
        .actionable_renews(store_clock_ms().expect("clock"))
        .expect("actionable")
        .into_iter()
        .find(|item| item.renew_id == renew_id)
        .expect("the cycle is still live");
    assert_eq!(
        settled.phase,
        RenewPhase::Ready,
        "and the cycle is authorised to clear"
    );
    server.join().expect("server");
}

#[test]
fn idle_occupancy_does_not_exhaust_an_every_interval() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let every_ms = 45 * 60 * 1_000;
    let mut intent = renew_intent(&worker, Some(every_ms));
    let armed_at = store_clock_ms().expect("clock");
    intent.scheduled_at_ms = armed_at + every_ms;
    let policy = store.create_renew(&intent).expect("create");
    let remaining = store.scheduled_interval_renews().expect("clocks")[0].active_remaining_ms;

    store
        .accrue_renew_occupancy(policy, false, armed_at + every_ms + 60_000)
        .expect("idle sample");
    assert!(
        store
            .actionable_renews(armed_at + every_ms + 60_000)
            .expect("actionable")
            .is_empty(),
        "wall-clock idle must not enter Preparing"
    );
    let clocks = store.scheduled_interval_renews().expect("clocks");
    assert_eq!(clocks[0].active_remaining_ms, remaining);
}

#[test]
fn an_unobserved_working_gap_does_not_exhaust_an_every_interval() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let every_ms = 45 * 60 * 1_000;
    let mut intent = renew_intent(&worker, Some(every_ms));
    let armed_at = store_clock_ms().expect("clock");
    intent.scheduled_at_ms = armed_at + every_ms;
    let policy = store.create_renew(&intent).expect("create");
    let remaining = store.scheduled_interval_renews().expect("clocks")[0].active_remaining_ms;

    store
        .accrue_renew_occupancy(policy, true, armed_at + every_ms)
        .expect("stale working sample");
    assert!(
        store
            .actionable_renews(armed_at + every_ms)
            .expect("actionable")
            .is_empty(),
        "a kelpied-down gap must not count as observed occupancy"
    );
    let clocks = store.scheduled_interval_renews().expect("clocks");
    assert_eq!(
        clocks[0].active_remaining_ms,
        remaining - kelpie::store::RENEW_OCCUPANCY_MAX_CREDIT_MS,
        "a long outage credits at most one sampling bound, never the whole interval"
    );
}

#[test]
fn working_occupancy_exhausts_an_every_interval() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let every_ms = 45 * 60 * 1_000;
    let mut intent = renew_intent(&worker, Some(every_ms));
    let armed_at = store_clock_ms().expect("clock");
    intent.scheduled_at_ms = armed_at + every_ms;
    let policy = store.create_renew(&intent).expect("create");

    earn_interval(&mut store, policy, every_ms);
    let due = store
        .actionable_renews(store_clock_ms().expect("clock"))
        .expect("actionable");
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].renew_id, policy);
    assert_eq!(due[0].phase, RenewPhase::Scheduled);
    assert_eq!(due[0].active_remaining_ms, Some(0));
}

#[test]
fn an_in_flight_cycle_is_not_paused_by_idle_occupancy() {
    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let renew_id = store
        .create_renew(&renew_intent(&worker, Some(45 * 60 * 1_000)))
        .expect("create");
    armed_prepare(&mut store, &worker, renew_id);

    let err = store
        .accrue_renew_occupancy(renew_id, false, store_clock_ms().expect("clock") + 1)
        .expect_err("preparing is not this clock");
    assert!(matches!(err, kelpie::store::StoreError::Conflict(_)));
    let item = store
        .actionable_renews(store_clock_ms().expect("clock"))
        .expect("actionable")
        .into_iter()
        .find(|item| item.renew_id == renew_id)
        .expect("still owed");
    assert_eq!(item.phase, RenewPhase::Preparing);
}

#[test]
fn idle_herdr_status_does_not_deliver_a_prepare() {
    let directory = tempfile::tempdir().expect("tempdir");
    let socket = directory.path().join("herdr.sock");
    let listener = UnixListener::bind(&socket).expect("bind");
    let server = thread::spawn(move || {
        serve_briefly(&listener, |request| {
            assert_eq!(request["method"], "session.snapshot");
            serde_json::json!({
                "type": "session_snapshot",
                "snapshot": {
                    "protocol": 20,
                    "agents": [{
                        "terminal_id": "term-1",
                        "pane_id": "w:p1",
                        "name": "worker",
                        "agent": "claude",
                        "agent_status": "idle",
                        "interactive_ready": true,
                        "launch_pending": false
                    }]
                }
            })
        })
    });

    let mut store = Store::in_memory().expect("store");
    let worker = ready(&mut store, "worker", "w:p1", "term-1", "start", "claude");
    let every_ms = 500;
    let mut intent = renew_intent(&worker, Some(every_ms));
    intent.scheduled_at_ms = store_clock_ms().expect("clock") + every_ms;
    store.create_renew(&intent).expect("create");
    let remaining = store.scheduled_interval_renews().expect("clocks")[0].active_remaining_ms;

    let mut kelpie = Kelpie::new(store, HerdrClient::new(&socket, Duration::from_secs(1)));
    assert_eq!(kelpie.drive_renews().expect("drive"), 0);
    thread::sleep(Duration::from_millis(5));
    assert_eq!(kelpie.drive_renews().expect("drive again"), 0);
    assert_eq!(
        kelpie
            .store_mut()
            .scheduled_interval_renews()
            .expect("clocks")[0]
            .active_remaining_ms,
        remaining,
        "an idle sample must consume no remaining time"
    );
    assert!(
        kelpie
            .store_mut()
            .actionable_renews(store_clock_ms().expect("clock"))
            .expect("actionable")
            .is_empty()
    );
    drop(kelpie);
    let requests = server.join().expect("server");
    assert!(
        !requests.is_empty(),
        "remaining time at the sample bound must ask Herdr"
    );
    assert!(
        requests
            .iter()
            .all(|request| request["method"] == "session.snapshot"),
        "idle occupancy must not submit a prepare: {requests:?}"
    );
}
